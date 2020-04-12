# reloader

A lambda to automatically reload new partitions into Athena for Cloudtrail logs

[![Codefresh build status]( https://g.codefresh.io/api/badges/pipeline/moonmoon1919/reloader%2Ftest?branch=master&key=eyJhbGciOiJIUzI1NiJ9.NWIyYThiMjYzYmFlOGEwMDAxY2RiZWZh.5h81Od2ooleQPSDJ1tUbMIrDYzxsRi3ovMy-NHkYNdY&type=cf-2)]( https%3A%2F%2Fg.codefresh.io%2Fpipelines%2Ftest%2Fbuilds%3FrepoOwner%3DMoonMoon1919%26repoName%3Dreloader%26serviceName%3DMoonMoon1919%252Freloader%26filter%3Dtrigger%3Abuild~Build%3Bbranch%3Amaster%3Bpipeline%3A5e92addb4c3d6b7faa5ac8d7~test)

---

## Why
- Athena can be extremely expensive at scale
- Adding partitions by region, year, day, and month drastically reduces the amount of data you will (likely) query
- Cloudtrail logs are extremely useful but companies rarely have them setup in a way that is convenient to query

---

## How To
TODO: Write me

---

## TO DO:
- [x] Add Codefresh for CI
- [ ] Rebuild partition checking logic to use better query
- [ ] Add aggressive retry logic when query is stuck in queued for longer than N periods
- [ ] Finish unit tests
- [ ] Bootstrap Install
- [ ] Auto set expiration to 180 days
- [ ] Log Forwarder to a single "security" account?

---

New Query for checking partition (see todo item above):

```
SELECT kv['region'] AS region, kv['year'] AS year, kv['month'] AS month, kv['day'] AS day
FROM
    (SELECT partition_number,
         map_agg(partition_key,
         partition_value) kv
    FROM information_schema.__internal_partitions__
    WHERE table_schema = '<table_schema>'
            AND table_name = '<table_name>'
    GROUP BY  partition_number)
WHERE kv['region'] = '<region>' AND kv['year'] = '<year>' AND kv['month'] = '<month>' AND kv['day'] = '<day>'
```

If response is none, create the partition!
