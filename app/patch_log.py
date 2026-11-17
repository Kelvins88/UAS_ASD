import sys
p = sys.argv[1]
s = open(p).read()

old1 = '''    state = {"rate": DEFAULT_RATE, "ttl_until": 0.0}
    t0 = time.time()'''
new1 = '''    state = {"rate": DEFAULT_RATE, "ttl_until": 0.0, "anom": False, "raised": False,
             "ep_start": 0.0}
    t0 = time.time()'''

old2 = '''        if temp > ANOMALY_TEMP and mode == "default":
            rate, mode = MAX_RATE / 2, "local_boost"
'''
new2 = '''        if temp > ANOMALY_TEMP and rate < MAX_RATE / 2:
            rate, mode = MAX_RATE / 2, "local_boost"
        # LOG PENGUKURAN (stdout, tidak bergantung pada broker)
        if anomaly and not state["anom"]:
            start = now - ((now - t0 + offset) % ANOMALY_EVERY_S)
            state.update(anom=True, raised=False, ep_start=max(start, t0 + 20))
            log("anomaly_start", episode_start_ts=round(state["ep_start"], 3), temp_c=round(temp, 2))
        if anomaly and not state["raised"] and rate > DEFAULT_RATE:
            state["raised"] = True
            log("rate_raised", mechanism=mode, rate_hz=rate,
                delay_s=round(now - state["ep_start"], 2))
        if not anomaly and state["anom"]:
            state["anom"] = False
            log("anomaly_end", raised=state["raised"])
'''
for o in (old1, old2):
    assert s.count(o) == 1, "blok tidak cocok - agent.py Anda berbeda dari versi saya"
s = s.replace(old1, new1).replace(old2, new2)
open(p, "w").write(s)
print("patch OK")
