import sys
p = sys.argv[1]
s = open(p).read()
edits = [
 ('ANOMALY_DURATION_S = float(os.environ.get("ANOMALY_DURATION_S", "15"))\n',
  'ANOMALY_DURATION_S = float(os.environ.get("ANOMALY_DURATION_S", "15"))\n'
  'SUCCESS_WINDOW_S = float(os.environ.get("SUCCESS_WINDOW_S", "3"))\n'),
 ('"ep_start": 0.0}',
  '"ep_start": 0.0, "delay": None, "ep_total": 0, "ep_ok": 0}'),
 ('            state["raised"] = True\n',
  '            state["raised"] = True\n'
  '            state["delay"] = round(now - state["ep_start"], 2)\n'),
 ('            state["anom"] = False\n            log("anomaly_end", raised=state["raised"])\n',
  '            state["anom"] = False\n'
  '            state["ep_total"] += 1\n'
  '            ok = state["raised"] and state["delay"] is not None and state["delay"] <= SUCCESS_WINDOW_S\n'
  '            state["ep_ok"] += int(ok)\n'
  '            log("anomaly_end", raised=state["raised"], success=ok)\n'),
 ('"rate_hz": rate, "mode": mode})',
  '"rate_hz": rate, "mode": mode,\n'
  '                              "ep_total": state["ep_total"], "ep_ok": state["ep_ok"]})'),
]
for o, n in edits:
    assert s.count(o) == 1, "blok tidak cocok: " + o[:40]
    s = s.replace(o, n)
open(p, "w").write(s)
print("patch2 OK")
