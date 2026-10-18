#!/usr/bin/env python3
"""Fetch timestamped public JRA odds for one race; never substitute stale snapshots."""
import datetime as dt, json, pathlib, re, time, urllib.parse, urllib.request
ID="202608040711"
OUT=pathlib.Path("results/live_odds")
OUT.mkdir(parents=True,exist_ok=True)
headers={"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
"Accept":"application/json,text/javascript,*/*","Referer":"https://race.netkeiba.com/odds/index.html?race_id="+ID}
market={}
errors={}
for kind in ("all","1","3","4","5","6","7","8"):
 params=urllib.parse.urlencode({"race_id":ID,"type":kind,"action":"init","sort":"odds","compress":"0","output":"json"})
 url="https://race.netkeiba.com/api/api_get_jra_odds.html?"+params
 try:
  req=urllib.request.Request(url,headers=headers)
  with urllib.request.urlopen(req,timeout=20) as response:
   payload=response.read(5000000).decode("utf-8-sig",errors="replace")
  try:obj=json.loads(payload)
  except json.JSONDecodeError:
   m=re.search(r"^[^(]+\((.*)\)\s*;?\s*$",payload,re.S)
   if not m:raise ValueError("Non-JSON API response (head="+repr(payload[:100])+")")
   obj=json.loads(m.group(1))
  keys=obj.get("data",{}).get("odds",{})
  count={str(t):len(v) for t,v in keys.items() if isinstance(v,dict)}
  market[kind]={"status":obj.get("status"),"time":obj.get("data",{}).get("official_datetime"),
  "odds":keys,"counts":count,"api_url":url}
  print("TYPE",kind,"status",obj.get("status"),"clock",market[kind]["time"],"sizes",count,flush=True)
 except Exception as e:
  errors[kind]=str(e)
  print("FAILED",kind,str(e),flush=True)
 time.sleep(.25)
stamp=dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat()
raw={"collection_timestamp_jst":stamp,"race_id":ID,"source":"netkeiba JRA-odds API (not necessarily JRA-official)",
 "response_by_requested_type":market,"errors":errors}
(OUT/"all_pools_snapshot.json").write_text(json.dumps(raw,ensure_ascii=False,indent=2),encoding="utf-8")
metadata={"collection_timestamp_jst":stamp,"race_id":ID,"source":"netkeiba",
 "official_times":{k:v["time"] for k,v in market.items()},
 "counts":{k:v["counts"] for k,v in market.items()},"errors":errors}
(OUT/"snapshot_audit.json").write_text(json.dumps(metadata,ensure_ascii=False,indent=2),encoding="utf-8")
if not market or all(not v["counts"] for v in market.values()):
 raise SystemExit("No usable public odds; do not make expected-value claims.")
