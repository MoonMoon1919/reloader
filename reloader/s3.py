from functools import cached_property
from typing import Optional

import boto3
from botocore.exceptions import ClientError


class S3Helper:
    def __init__(self, bucket: str, account_id: str):
        self._bucket = bucket
        self._account_id = account_id
        self._client = boto3.client("s3")

    @cached_property
    def regions(self):
        return self._get_regions()

    @cached_property
    def experation_after_days(self):
        return self._get_bucket_lifecycle_expiration()

    def _get_bucket_lifecycle_expiration(self) -> Optional[int]:
        """Given a bucket, retrieve the amount of days before expiration."""
        try:
            resp = self._client.get_bucket_lifecycle_configuration(Bucket=self._bucket)
        except ClientError as error:
            print(error.response)
            return None
        else:
            return self._parse_lifeycle_rules_for_expiration(resp.get("Rules", []))

    def _parse_lifeycle_rules_for_expiration(self, rule_list: list) -> Optional[int]:
        """Iterate through list of rules to locate the expiration policy."""
        for rule in rule_list:
            if (expiration_days := rule.get("Expiration", {}).get("Days", None)) is not None:
                return int(expiration_days)
            else:
                pass

        return None

    def _get_regions(self) -> list:
        """Given a bucket, retrieve a list of common prefixes for cloudtrail logs."""
        try:
            resp = self._client.list_objects(
                Bucket=self._bucket, Prefix=f"AWSLogs/{self._account_id}/CloudTrail/", Delimiter="/"
            )
        except ClientError as error:
            print(error.response)
            return []
        else:
            return self._retrieve_regions(prefixes=resp["CommonPrefixes"])

    def _retrieve_regions(self, prefixes: list) -> list:
        """Retrieve a list of regions from the returned common prefixes."""
        regions = []

        for prefix in prefixes:
            pfix = prefix.get("Prefix", None)

            if pfix:
                splitter = pfix.split("/")

                try:
                    region = splitter[3]
                except IndexError:
                    print("unable to find region")
                else:
                    regions.append(region)

        return regions
