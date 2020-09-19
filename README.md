# reloader

A lambda to automatically reload new partitions into Athena for Cloudtrail logs

[![Codefresh build status]( https://g.codefresh.io/api/badges/pipeline/moonmoon1919/reloader%2Ftest?branch=master&key=eyJhbGciOiJIUzI1NiJ9.NWIyYThiMjYzYmFlOGEwMDAxY2RiZWZh.5h81Od2ooleQPSDJ1tUbMIrDYzxsRi3ovMy-NHkYNdY&type=cf-2)]( https%3A%2F%2Fg.codefresh.io%2Fpipelines%2Ftest%2Fbuilds%3FrepoOwner%3DMoonMoon1919%26repoName%3Dreloader%26serviceName%3DMoonMoon1919%252Freloader%26filter%3Dtrigger%3Abuild~Build%3Bbranch%3Amaster%3Bpipeline%3A5e92addb4c3d6b7faa5ac8d7~test)

---

## Why
- Adding partitions by region, year, day, and month reduces the amount of data you will (likely) query
- Improves query performance on point queries and queries returning a small dataset
- Cloudtrail logs are useful but are rarely set up in a way that is convenient to query

---

## How To
- Create a lambda with the code in this package
- Add the environment variables below
- Run at midnight every day via AWS EventBridge Rule

Envrionment Variables
```
BUCKET="<name of your cloudtrail logs S3 bucket>"
LOG_LOCATION="AWSLogs"
ACCOUNT_ID="<your account id>"
DATABASE="<name of athena database containing table>"
TABLE_NAME="cloudtrail_logs"
OUTPUT_LOC="s3://<athena output bucket>/<output loc>/"
```

---

## TO DO:
- [ ] Bootstrap Install
- [ ] Alerts/Notifications
- [ ] Optional quicksight dashboards
