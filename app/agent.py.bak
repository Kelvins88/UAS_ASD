"""Agen edge (1 container = 1 agen). AGENT_ROLE: sensor | coordinator.
Topic  : edge/<GROUP>/<AGENT_ID>/telemetry|cmd|status
Payload: JSON berisi ts + seq. Status: heartbeat >=1 Hz retained; LWT=offline.
"""
import json, os, random, signal, threading, time, zlib
from collections import deque
from http.server import BaseHTTPRequestHandler, HTTPServer

import paho.mqtt.client as mqtt

GROUP = os.environ["GROUP"]
AGENT_ID = os.environ["AGENT_ID"]
ROLE = os.environ.get("AGENT_ROLE", "sensor")
HOST = os.environ.get("MQTT_HOST", "mosquitto")
PORT = int(os.environ.get("MQTT_PORT", "1883"))
USER = os.environ["MQTT_USER"]
PASSWD = os.environ["MQTT_PASS"]
HB_HZ = max(1.0, float(os.environ.get("HEARTBEAT_HZ", "1")))
DEFAULT_RATE = float(os.environ.get("DEFAULT_RATE_HZ", "0.5"))
MIN_RATE, MAX_RATE = 0.2, 5.0
ANOMALY_TEMP = float(os.environ.get("ANOMALY_TEMP_C", "35"))
ANOMALY_EVERY_S = float(os.environ.get("ANOMALY_EVERY_S", "60"))
ANOMALY_DURATION_S = float(os.environ.get("ANOMALY_DURATION_S", "15"))


def T(agent, kind):
    return f"edge/{GROUP}/{agent}/{kind}"


stop = threading.Event()
tick = time.time()
seqs = {"telemetry": 0, "status": 0, "cmd": 0}
seq_lock = threading.Lock()


def log(event, level="INFO", **kw):
    print(json.dumps({"ts": round(time.time(), 3), "level": level, "agent": AGENT_ID,
                      "event": event, **kw}), flush=True)


def next_seq(kind):
    with seq_lock:
        seqs[kind] += 1
        return seqs[kind]


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"{GROUP}-{AGENT_ID}")
client.username_pw_set(USER, PASSWD)
client.max_queued_messages_set(2000)
client.reconnect_delay_set(min_delay=1, max_delay=5)
client.will_set(T(AGENT_ID, "status"),
                json.dumps({"ts": 0, "seq": 0, "agent": AGENT_ID, "state": "offline",
                            "reason": "lwt"}), qos=1, retain=True)
connected = threading.Event()
handlers = []


def on_connect(c, _u, _f, rc, _p=None):
    if rc == 0:
        connected.set()
        log("mqtt_connected", host=HOST)
        for flt, _ in handlers:
            c.subscribe(flt, qos=1)
    else:
        log("mqtt_connect_failed", "WARN", rc=str(rc))


def on_disconnect(_c, _u, _f, rc, _p=None):
    connected.clear()
    log("mqtt_disconnected", "WARN", rc=str(rc))


def on_message(_c, _u, msg):
    try:
        data = json.loads(msg.payload)
    except ValueError:
        return log("bad_payload", "WARN", topic=msg.topic)
    for flt, fn in handlers:
        if mqtt.topic_matches_sub(flt, msg.topic):
            fn(msg.topic, data)


client.on_connect, client.on_disconnect, client.on_message = on_connect, on_disconnect, on_message


def publish(kind, body, qos=1, retain=False, target=None):
    body = {"ts": round(time.time(), 3), "seq": next_seq(kind), "agent": AGENT_ID, **body}
    client.publish(T(target or AGENT_ID, kind), json.dumps(body), qos=qos, retain=retain)
    return body


def heartbeat_loop():
    while not stop.is_set():
        publish("status", {"state": "online", "role": ROLE}, qos=0, retain=True)
        stop.wait(1.0 / HB_HZ)


class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":    # liveness: tidak bergantung pada broker
            ok = time.time() - tick < 15
        elif self.path == "/readyz":   # readiness: sudah terhubung ke broker
            ok = connected.is_set()
        else:
            ok = False
        self.send_response(200 if ok else 503)
        self.end_headers()
        self.wfile.write(b"ok" if ok else b"fail")

    def log_message(self, *_):
        pass


def run_sensor():
    global tick
    state = {"rate": DEFAULT_RATE, "ttl_until": 0.0}
    t0 = time.time()
    offset = zlib.crc32(AGENT_ID.encode()) % 20

    def on_cmd(_t, d):
        if d.get("action") == "set_rate":
            r = min(MAX_RATE, max(MIN_RATE, float(d.get("rate_hz", DEFAULT_RATE))))
            state["rate"] = r
            state["ttl_until"] = time.time() + float(d.get("ttl_s", 15))
            log("cmd_applied", rate_hz=r, cmd_seq=d.get("seq"))

    handlers.append((T(AGENT_ID, "cmd"), on_cmd))
    log("sensor_start", default_rate=DEFAULT_RATE)
    next_t = time.time()
    while not stop.is_set():
        now = tick = time.time()
        # FAIL-SAFE: perintah kedaluwarsa (koordinator/broker hilang) -> kembali ke default
        mode = "cmd" if now < state["ttl_until"] else "default"
        rate = state["rate"] if mode == "cmd" else DEFAULT_RATE
        anomaly = ((now - t0 + offset) % ANOMALY_EVERY_S) < ANOMALY_DURATION_S and now - t0 > 20
        temp = 27 + random.gauss(0, 0.4) + (10 if anomaly else 0)
        hum = 60 + random.gauss(0, 1.5)
        # OTONOMI LOKAL: anomali terdeteksi lokal -> naikkan sampling tanpa koordinator
        if temp > ANOMALY_TEMP and mode == "default":
            rate, mode = MAX_RATE / 2, "local_boost"
        publish("telemetry", {"temp_c": round(temp, 2), "hum_pct": round(hum, 2),
                              "rate_hz": rate, "mode": mode})
        next_t += 1.0 / rate
        if next_t < time.time() - 5:
            next_t = time.time()
        stop.wait(max(0.0, next_t - time.time()))


def run_coordinator():
    global tick
    nodes, lat = {}, deque(maxlen=500)
    lock = threading.Lock()

    def node(a):
        return nodes.setdefault(a, {"last_seq": None, "rx": 0, "lost": 0, "hb": 0.0,
                                    "down_since": None, "high": False, "last_cmd": 0.0})

    def on_tel(topic, d):
        a = topic.split("/")[2]
        with lock:
            n = node(a)
            if n["last_seq"] is not None:
                if d["seq"] > n["last_seq"]:
                    n["lost"] += d["seq"] - n["last_seq"] - 1
                else:
                    log("agent_restart_detected", peer=a)
            n["last_seq"], n["rx"] = d["seq"], n["rx"] + 1
            hot = d["temp_c"] > ANOMALY_TEMP
            changed = hot != n["high"]
            if (hot or changed) and (changed or time.time() - n["last_cmd"] > 5):
                n["high"], n["last_cmd"] = hot, time.time()
                rate = MAX_RATE if hot else DEFAULT_RATE
                publish("cmd", {"action": "set_rate", "rate_hz": rate, "ttl_s": 15,
                                "reason": "anomaly" if hot else "recovered"}, target=a)
                lat.append((time.time() - d["ts"]) * 1000)
                log("decision", peer=a, rate_hz=rate, latency_ms=round(lat[-1], 1))

    def on_status(topic, d):
        a = topic.split("/")[2]
        if a == AGENT_ID or d.get("state") != "online":
            return
        with lock:
            n = node(a)
            if n["down_since"]:
                log("agent_recovered", peer=a, mttr_s=round(time.time() - n["down_since"], 2))
                n["down_since"] = None
            n["hb"] = time.time()

    handlers.extend([(T("+", "telemetry"), on_tel), (T("+", "status"), on_status)])
    log("coordinator_start", anomaly_temp=ANOMALY_TEMP)
    last_report = time.time()
    while not stop.is_set():
        now = tick = time.time()
        with lock:
            for a, n in nodes.items():
                if n["hb"] and not n["down_since"] and now - n["hb"] > 3.0 / HB_HZ:
                    n["down_since"] = n["hb"]
                    log("agent_down", "WARN", peer=a)
            if now - last_report >= 10 and nodes:
                last_report = now
                s = sorted(lat)

                def pct(p):
                    return round(s[min(len(s) - 1, int(p * len(s)))], 1) if s else None
                rx = sum(n["rx"] for n in nodes.values())
                ls = sum(n["lost"] for n in nodes.values())
                log("metrics", agents=len(nodes), rx=rx, lost=ls,
                    loss_pct=round(100 * ls / max(1, rx + ls), 2),
                    decision_p50_ms=pct(0.5), decision_p95_ms=pct(0.95))
        stop.wait(1.0)


def shutdown(*_):
    log("sigterm")
    try:  # offline eksplisit (disconnect bersih tidak memicu LWT)
        publish("status", {"state": "offline", "reason": "shutdown"}, qos=1, retain=True)
        time.sleep(0.3)
    finally:
        stop.set()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    threading.Thread(target=lambda: HTTPServer(("", 8080), Health).serve_forever(),
                     daemon=True).start()
    loop_fn = run_sensor if ROLE == "sensor" else run_coordinator

    def connect_later():  # tunggu handler terdaftar, lalu konek (retry otomatis)
        time.sleep(0.3)
        client.connect_async(HOST, PORT, keepalive=5)
        client.loop_start()
    threading.Thread(target=connect_later, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    loop_fn()
    client.loop_stop()
