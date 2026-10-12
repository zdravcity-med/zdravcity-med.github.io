#!/usr/bin/env python3
"""Очередь на биоимпеданс: запись по QR, живая очередь, панель оператора.

Запуск: python3 server.py [--port 8765] [--pin 1234]
Состояние хранится в queue.json рядом со скриптом.
"""
import argparse
import csv
import io
import json
import os
import secrets
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

SLOT_MIN = 15      # сама процедура
BUFFER_MIN = 5     # запас на задержку
STEP = SLOT_MIN + BUFFER_MIN
ROUND_MIN = 5      # время начала округляем вверх до 5 минут
OPEN_MIN = 10 * 60  # стенд открывается в 10:00, раньше талоны не выдаём
CLOSE_MIN = 18 * 60  # стенд работает до 18:00: процедура должна закончиться к этому времени
HORIZON_MIN = 24 * 60  # горизонт записи; 60 = только на ближайший час (для дней с наплывом), 24*60 = без ограничения
GRACE_MIN = 5        # льгота после времени талона (вызов на табло, "талон действует до")
AUTO_EXPIRE = False  # True: не подошёл за GRACE_MIN - талон сгорает сам; False: неявку отмечает оператор

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(HERE, "queue.json")
LOCK = threading.Lock()
STATE = {"entries": [], "seq": 0, "breaks": []}

ACTIVE = ("waiting", "inprogress")


def load():
    global STATE
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, encoding="utf-8") as f:
            STATE = json.load(f)


def save():
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(STATE, f, ensure_ascii=False, indent=1)
    os.replace(tmp, STATE_FILE)


def today():
    return datetime.now().strftime("%Y-%m-%d")


def ensure_day():
    """Очередь живёт один день: при первом запросе нового дня вчерашние записи
    уходят в бэкап, нумерация талонов начинается заново. Вызывать под LOCK."""
    if STATE.get("day") == today():
        return
    if STATE["entries"]:
        backup = os.path.join(HERE, "queue-%s.json" % STATE.get("day", "old"))
        with open(backup, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=1)
    STATE["entries"] = []
    STATE["seq"] = 0
    STATE["breaks"] = []
    STATE["day"] = today()
    save()


def expire():
    """Сжигает талоны, к которым не подошли за GRACE_MIN после времени, и подтягивает
    очередь. Вызывать под LOCK при каждом запросе (страницы опрашивают сервер каждые 5 с)."""
    if not AUTO_EXPIRE:
        return
    now = now_min()
    burned = [e for e in STATE["entries"] if e["status"] == "waiting" and e["slot"] + GRACE_MIN < now]
    if not burned:
        return
    for e in burned:
        e["status"] = "noshow"
        e["auto"] = True
    compact()
    save()


def now_min():
    """Текущее время в минутах от полуночи (локальное)."""
    t = datetime.now()
    return t.hour * 60 + t.minute + t.second / 60


def ceil_round(m):
    return int(-(-m // ROUND_MIN) * ROUND_MIN)


def breaks():
    return STATE.setdefault("breaks", [])


def fit(t, busy):
    """Сдвигает начало t вперёд, пока окно [t, t+STEP) пересекается с занятыми
    интервалами или сама процедура [t, t+SLOT_MIN) попадает в перерыв."""
    changed = True
    while changed:
        changed = False
        for a, b in busy:
            if a < t + STEP and t < b:
                t = ceil_round(b)
                changed = True
        for br in breaks():
            if br["start"] < t + SLOT_MIN and t < br["end"]:
                t = ceil_round(br["end"])
                changed = True
    return t


def first_free_slot():
    """Первое окно длиной STEP, начиная не раньше текущего момента, которое
    не пересекается с активными записями и перерывами. Освободившиеся дырки заполняются.
    None - запись на сегодня закрыта (не успеть до закрытия)."""
    busy = sorted((e["slot"], e["slot"] + STEP) for e in STATE["entries"] if e["status"] in ACTIVE)
    t = fit(max(ceil_round(now_min()), OPEN_MIN), busy)
    return t if t + SLOT_MIN <= CLOSE_MIN else None


def reflow(priority_id=None):
    """Пересобирает времена ожидающих талонов после перерыва или ручного переноса:
    порядок по времени сохраняется, каждый талон сдвигается вперёд, если попал
    в перерыв или наехал на предыдущий. Назад талоны не двигаются, чтобы человек
    не пришёл позже уже прошедшего времени. priority_id - только что перенесённый
    талон, при совпадении времени он идёт первым. Вызывать под LOCK."""
    fixed = [(e["slot"], e["slot"] + STEP) for e in STATE["entries"] if e["status"] == "inprogress"]
    moved = []
    for e in sorted((e for e in STATE["entries"] if e["status"] == "waiting"), key=lambda e: (e["slot"], e["id"] != priority_id, e["id"])):
        t = fit(e["slot"], fixed)
        if t != e["slot"]:
            e["slot"] = t
            e["moved"] = True
            moved.append(e["num"])
        fixed.append((t, t + STEP))
    return moved


PULL_NOTICE = 20    # подтягиваем раньше только если до нового времени >= 20 минут...
PULL_FRESH = 30     # ...или человек записался в последние 30 минут (ещё у стенда)


def compact():
    """Подтягивает ожидающие талоны на пустые окна в порядке номеров: кто записался
    раньше, тот первым получает освободившееся время. Двигаем только в окна, которые
    никто не держит, поэтому никого не отодвигаем позже. Талон двигается раньше,
    только если человек записался недавно или до нового времени достаточно времени,
    чтобы увидеть перенос на телефоне. Перенесённые оператором вручную не трогаем.
    Вызывать под LOCK после reflow()."""
    now = now_min()
    waiting = sorted((e for e in STATE["entries"] if e["status"] == "waiting"), key=lambda e: e["id"])
    occupied = {e["id"]: (e["slot"], e["slot"] + STEP) for e in STATE["entries"] if e["status"] in ACTIVE}
    moved = []
    for e in waiting:
        others = [iv for i, iv in occupied.items() if i != e["id"]]
        earliest = fit(max(ceil_round(now), OPEN_MIN), others)
        fresh = (now - e.get("createdMin", 0)) <= PULL_FRESH
        if earliest < e["slot"] and not e.get("pinned") and (fresh or earliest >= now + PULL_NOTICE):
            e["slot"] = earliest
            e["moved"] = True
            moved.append(e["num"])
            occupied[e["id"]] = (earliest, earliest + STEP)
    return moved


def current_break():
    now = now_min()
    upcoming = sorted((b for b in breaks() if b["end"] > now), key=lambda b: b["start"])
    return upcoming[0] if upcoming else None


def parse_time(s):
    h, m = str(s).strip().split(":")
    v = int(h) * 60 + int(m)
    if not 0 <= v < 24 * 60:
        raise ValueError
    return v


def fmt(m):
    return "%02d:%02d" % (int(m) // 60 % 24, int(m) % 60)


def ticket(n):
    return "Б-%03d" % n


def mask(name):
    parts = name.split()
    if len(parts) >= 2:
        return parts[0] + " " + parts[1][0] + "."
    return name


def public_view():
    active = sorted((e for e in STATE["entries"] if e["status"] in ACTIVE), key=lambda e: e["slot"])
    return {
        "now": fmt(now_min()),
        "step": STEP,
        "slotMin": SLOT_MIN,
        "nextFree": fmt(first_free_slot()) if first_free_slot() is not None else None,
        "closed": first_free_slot() is None,
        "opensAt": (fmt(first_free_slot() - HORIZON_MIN) if first_free_slot() is not None and first_free_slot() > now_min() + HORIZON_MIN else None),
        "closeTime": fmt(CLOSE_MIN),
        "grace": GRACE_MIN,
        "waiting": len(active),
        "break": ({"start": fmt(b["start"]), "end": fmt(b["end"]), "now": b["start"] <= now_min() < b["end"]} if (b := current_break()) else None),
        "queue": [{"id": e["id"], "num": e["num"], "name": mask(e["name"]), "time": fmt(e["slot"]), "status": e["status"]} for e in active],
    }


def admin_view():
    entries = sorted(STATE["entries"], key=lambda e: e["slot"])
    return {
        "now": fmt(now_min()),
        "nextFree": fmt(first_free_slot()) if first_free_slot() is not None else "закрыто",
        "entries": [dict(e, time=fmt(e["slot"])) for e in entries],
        "breaks": [dict(b, startText=fmt(b["start"]), endText=fmt(b["end"])) for b in sorted(breaks(), key=lambda b: b["start"])],
        "counts": {s: sum(1 for e in entries if e["status"] == s) for s in ("waiting", "inprogress", "done", "noshow", "cancelled")},
    }


class Handler(BaseHTTPRequestHandler):
    pin = "1234"

    def log_message(self, *a):
        pass

    # helpers
    def send(self, code, body, ctype="application/json; charset=utf-8", cookie=None):
        data = body if isinstance(body, bytes) else json.dumps(body, ensure_ascii=False).encode()
        self.send_response(code)
        if cookie:
            self.send_header("Set-Cookie", cookie)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def html(self, name):
        with open(os.path.join(HERE, name), "rb") as f:
            self.send(200, f.read(), "text/html; charset=utf-8")

    def body_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def device(self):
        """Идентификатор устройства в cookie: один активный талон на устройство,
        после прохождения повторная запись в этот день закрыта. Инкогнито обходит, это допустимо."""
        for part in self.headers.get("Cookie", "").split(";"):
            k, _, v = part.strip().partition("=")
            if k == "bq_dev" and len(v) >= 16:
                return v, None
        v = secrets.token_urlsafe(16)
        return v, "bq_dev=%s; Path=/; Max-Age=2592000; SameSite=Lax; HttpOnly" % v

    def is_admin(self):
        return secrets.compare_digest(self.headers.get("X-Pin", ""), self.pin)

    def do_GET(self):
        u = urlparse(self.path)
        with LOCK:
            ensure_day()
            expire()
        if u.path == "/":
            return self.html("index.html")
        if u.path == "/admin":
            return self.html("admin.html")
        if u.path == "/board":
            return self.html("board.html")
        if u.path == "/api/state":
            with LOCK:
                return self.send(200, public_view())
        if u.path == "/api/admin/state":
            if not self.is_admin():
                return self.send(403, {"error": "bad pin"})
            with LOCK:
                return self.send(200, admin_view())
        if u.path == "/api/admin/export.csv":
            q = parse_qs(u.query)
            if not secrets.compare_digest(q.get("pin", [""])[0], self.pin):
                return self.send(403, {"error": "bad pin"})
            buf = io.StringIO()
            w = csv.writer(buf, delimiter=";")
            w.writerow(["Номер", "Время", "Имя", "Телефон", "Статус", "Записан"])
            with LOCK:
                for e in sorted(STATE["entries"], key=lambda e: e["slot"]):
                    w.writerow([e.get("num", ""), fmt(e["slot"]), e["name"], e.get("phone", ""), e["status"], e["created"]])
            data = buf.getvalue().encode("utf-8-sig")
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", "attachment; filename=bioimpedance.csv")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        self.send(404, {"error": "not found"})

    def do_POST(self):
        u = urlparse(self.path)
        with LOCK:
            ensure_day()
            expire()
        try:
            body = self.body_json()
        except Exception:
            return self.send(400, {"error": "bad json"})

        if u.path == "/api/join":
            name = (body.get("name") or "").strip()[:60]
            phone = (body.get("phone") or "").strip()[:30]
            dev, cookie = self.device()
            with LOCK:
                mine = [e for e in STATE["entries"] if e.get("dev") == dev]
                active = next((e for e in mine if e["status"] in ACTIVE), None)
                if active:
                    return self.send(409, {"error": "У вас уже есть талон %s" % active["num"], "code": "active",
                                           "id": active["id"], "num": active["num"], "token": active["token"], "time": fmt(active["slot"])}, cookie=cookie)
                done = next((e for e in mine if e["status"] == "done"), None)
                if done:
                    return self.send(409, {"error": "Вы уже прошли биоимпеданс сегодня", "code": "done",
                                           "id": done["id"], "num": done["num"], "token": done["token"]}, cookie=cookie)
                if sum(1 for e in STATE["entries"] if e["status"] in ACTIVE) >= 200:
                    return self.send(409, {"error": "Очередь заполнена"}, cookie=cookie)
                slot = first_free_slot()
                if slot is None:
                    return self.send(409, {"error": "Запись на сегодня закрыта: стенд работает до %s" % fmt(CLOSE_MIN), "code": "closed"}, cookie=cookie)
                if slot > now_min() + HORIZON_MIN:
                    return self.send(409, {"error": "Запись на %s откроется в %s" % (fmt(slot), fmt(slot - HORIZON_MIN)), "code": "not_yet"}, cookie=cookie)
                STATE["seq"] += 1
                e = {
                    "id": STATE["seq"],
                    "num": ticket(STATE["seq"]),
                    "token": secrets.token_urlsafe(12),
                    "dev": dev,
                    "name": name,
                    "phone": phone,
                    "slot": first_free_slot(),
                    "status": "waiting",
                    "created": datetime.now().strftime("%H:%M"),
                    "createdMin": int(now_min()),
                }
                STATE["entries"].append(e)
                compact()
                save()
                return self.send(200, {"id": e["id"], "num": e["num"], "token": e["token"], "time": fmt(e["slot"])}, cookie=cookie)

        if u.path == "/api/me":
            dev, cookie = self.device()
            with LOCK:
                if body.get("id"):
                    e = next((x for x in STATE["entries"] if x["id"] == body.get("id") and x["token"] == body.get("token")), None)
                else:
                    mine = [x for x in STATE["entries"] if x.get("dev") == dev]
                    e = next((x for x in mine if x["status"] in ACTIVE), None) or next((x for x in mine if x["status"] == "done"), None)
                if not e:
                    return self.send(404, {"error": "not found"}, cookie=cookie)
                ahead = sum(1 for x in STATE["entries"] if x["status"] in ACTIVE and x["slot"] < e["slot"])
                return self.send(200, {"id": e["id"], "num": e["num"], "token": e["token"], "name": e["name"], "time": fmt(e["slot"]), "status": e["status"], "ahead": ahead, "moved": bool(e.get("moved")), "expiresAt": fmt(e["slot"] + GRACE_MIN), "auto": bool(e.get("auto"))}, cookie=cookie)

        if u.path == "/api/cancel":
            with LOCK:
                e = next((x for x in STATE["entries"] if x["id"] == body.get("id") and x["token"] == body.get("token")), None)
                if not e:
                    return self.send(404, {"error": "not found"})
                if e["status"] == "waiting":
                    e["status"] = "cancelled"
                    compact()
                    save()
                return self.send(200, {"ok": True})

        if u.path == "/api/admin/set":
            if not self.is_admin():
                return self.send(403, {"error": "bad pin"})
            status = body.get("status")
            if status not in ("waiting", "inprogress", "done", "noshow", "cancelled"):
                return self.send(400, {"error": "bad status"})
            with LOCK:
                e = next((x for x in STATE["entries"] if x["id"] == body.get("id")), None)
                if not e:
                    return self.send(404, {"error": "not found"})
                if status == "waiting" and e["slot"] + GRACE_MIN < now_min():
                    e["slot"] = ceil_round(now_min())
                    e["pinned"] = True
                e["status"] = status
                e.pop("auto", None)
                reflow(priority_id=e["id"])
                moved = compact()
                save()
                v = admin_view(); v["moved"] = moved
                return self.send(200, v)

        if u.path == "/api/admin/add":
            # оператор записывает подошедшего к стенду человека без телефона
            if not self.is_admin():
                return self.send(403, {"error": "bad pin"})
            name = (body.get("name") or "").strip()[:60]
            with LOCK:
                if first_free_slot() is None:
                    return self.send(409, {"error": "Запись на сегодня закрыта: стенд работает до %s" % fmt(CLOSE_MIN)})
                STATE["seq"] += 1
                STATE["entries"].append({
                    "id": STATE["seq"], "num": ticket(STATE["seq"]), "token": secrets.token_urlsafe(12), "name": name,
                    "phone": (body.get("phone") or "").strip()[:30], "slot": first_free_slot(),
                    "status": "waiting", "created": datetime.now().strftime("%H:%M"), "createdMin": int(now_min()),
                })
                compact()
                save()
                return self.send(200, admin_view())

        if u.path == "/api/admin/move":
            # ручной перенос: меняется только этот талон, остальные не сдвигаются;
            # занятое время или перерыв - отказ с указанием, кем занято
            if not self.is_admin():
                return self.send(403, {"error": "bad pin"})
            try:
                t = parse_time(body.get("time"))
            except Exception:
                return self.send(400, {"error": "Время в формате ЧЧ:ММ"})
            with LOCK:
                e = next((x for x in STATE["entries"] if x["id"] == body.get("id")), None)
                if not e or e["status"] != "waiting":
                    return self.send(404, {"error": "Талон не найден или уже не ждёт"})
                for x in STATE["entries"]:
                    if x["id"] != e["id"] and x["status"] in ACTIVE and x["slot"] < t + STEP and t < x["slot"] + STEP:
                        return self.send(409, {"error": "Время %s занято талоном %s (%s)" % (fmt(t), x["num"], fmt(x["slot"]))})
                for br in breaks():
                    if br["start"] < t + SLOT_MIN and t < br["end"]:
                        return self.send(409, {"error": "На %s перерыв %s-%s" % (fmt(t), fmt(br["start"]), fmt(br["end"]))})
                e["slot"] = t
                e["moved"] = True
                e["pinned"] = True
                save()
                return self.send(200, admin_view())

        if u.path == "/api/admin/break":
            # перерыв с start (ЧЧ:ММ, по умолчанию сейчас) на minutes; попавшие талоны сдвигаются
            if not self.is_admin():
                return self.send(403, {"error": "bad pin"})
            try:
                minutes = int(body.get("minutes") or 0)
                start = parse_time(body["start"]) if body.get("start") else int(now_min())
            except Exception:
                return self.send(400, {"error": "Время в формате ЧЧ:ММ, длительность в минутах"})
            if not 1 <= minutes <= 12 * 60:
                return self.send(400, {"error": "Длительность от 1 минуты до 12 часов"})
            with LOCK:
                bid = max([b["id"] for b in breaks()] + [0]) + 1
                breaks().append({"id": bid, "start": start, "end": start + minutes})
                moved = reflow()
                moved += [n for n in compact() if n not in moved]
                save()
                v = admin_view(); v["moved"] = moved
                return self.send(200, v)

        if u.path == "/api/admin/break/delete":
            if not self.is_admin():
                return self.send(403, {"error": "bad pin"})
            with LOCK:
                STATE["breaks"] = [b for b in breaks() if b["id"] != body.get("id")]
                moved = compact()
                save()
                v = admin_view(); v["moved"] = moved
                return self.send(200, v)

        if u.path == "/api/admin/reset":
            if not self.is_admin():
                return self.send(403, {"error": "bad pin"})
            with LOCK:
                backup = os.path.join(HERE, "queue-%s.json" % datetime.now().strftime("%Y%m%d-%H%M%S"))
                if STATE["entries"]:
                    with open(backup, "w", encoding="utf-8") as f:
                        json.dump(STATE, f, ensure_ascii=False, indent=1)
                STATE["entries"] = []
                STATE["seq"] = 0
                STATE["breaks"] = []
                STATE["day"] = today()
                save()
                return self.send(200, admin_view())

        self.send(404, {"error": "not found"})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--pin", default=os.environ.get("PIN", "1234"))
    a = ap.parse_args()
    Handler.pin = a.pin
    load()
    srv = ThreadingHTTPServer(("0.0.0.0", a.port), Handler)
    print("listening on http://0.0.0.0:%d  admin pin: %s" % (a.port, a.pin), flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
