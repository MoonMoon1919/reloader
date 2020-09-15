# reloader

A lambda to automatically reload new partitions into Athena for Cloudtrail logs

[![Codefresh build status]( https://g.codefresh.io/api/badges/pipeline/moonmoon1919/reloader%2Ftest?branch=master&key=eyJhbGciOiJIUzI1NiJ9.NWIyYThiMjYzYmFlOGEwMDAxY2RiZWZh.5h81Od2ooleQPSDJ1tUbMIrDYzxsRi3ovMy-NHkYNdY&type=cf-2)]( https%3A%2F%2Fg.codefresh.io%2Fpipelines%2Ftest%2Fbuilds%3FrepoOwner%3DMoonMoon1919%26repoName%3Dreloader%26serviceName%3DMoonMoon1919%252Freloader%26filter%3Dtrigger%3Abuild~Build%3Bbranch%3Amaster%3Bpipeline%3A5e92addb4c3d6b7faa5ac8d7~test)

---

## Why
- Athena can be expensive at scale
- Adding partitions by region, year, day, and month drastically reduces the amount of data you will (likely) query
- Cloudtrail logs are useful but are rarely set up in a way that is convenient to query

---

## How To
TODO: Write me

---

## TO DO:
- [x] Rewrite to handle based on Cron'd event
- [x] Add Codefresh for CI
- [x] Rebuild partition checking logic to use better query
- [ ] Add retry logic when query is stuck in queued for longer than N periods
- [x] Finish unit tests
- [ ] Bootstrap Install
