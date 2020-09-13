"""
Automatically load newly created S3 prefixes into Athena.
"""

# Import from std lib
from datetime import datetime
from functools import cached_property
import logging
import os
from time import sleep
from typing import TypedDict, Optional
import sys

# Import from
import boto3
from botocore.exceptions import ClientError

# Retrieve env vars
bucket = os.environ.get("BUCKET")
account_id = os.environ.get("ACCOUNT_ID")
database = os.environ.get("DATABASE")
table_name = os.environ.get("TABLE_NAME")
output_loc = os.environ.get("OUTPUT_LOC")

# Create the logger
logger = logging.getLogger("athena-cloudtrail-reloader")
logger.setLevel(logging.INFO)
default_handler = logging.StreamHandler(sys.stderr)
default_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(default_handler)


class ExecutionResponse(TypedDict):
    """Data structure for Athena execution response."""

    execution_id: str
    execution_res_loc: str


class Athena:
    """Class for interacting with AWS Athena."""

    def __init__(self, database: str, output_loc: str) -> None:
        self.database = database
        self.output_loc = output_loc

        self.client = boto3.client("athena")
        self.paginator = self.client.get_paginator("get_query_results")

    def results(self, execution_response: ExecutionResponse) -> dict:
        try:
            resp = self.paginator.paginate(QueryExecutionId=execution_response["execution_id"])
        except ClientError as e:
            raise e
        else:
            results = [result["ResultSet"] for result in resp]
        finally:
            return results

    def wait_for_completion(self, execution_response: ExecutionResponse) -> str:
        """Execute an Athena query and wait until it has succeeded, failed, or been cancelled.

        Args:
            - execution_response: an ExecutionResponse containing the execution ID

        Returns:
            - str: from ["SUCCEEDED", "FAILED", "CANCELLED"]
        """
        status = "RUNNING"

        while status not in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            try:
                resp = self.client.get_query_execution(QueryExecutionId=execution_response["execution_id"])
            except ClientError as e:
                raise e
            else:
                status = resp["QueryExecution"]["Status"]["State"]
                sleep(0.25)

        return status

    def _process_execution_response(self, execution_response: dict) -> ExecutionResponse:
        """Takes the execution response from Athena's start_query_execution call
        and converts it to a ExecutionResponse object

        Args:
            - execution_response: The execution response from start_query_execution

        Returns:
            - ExecutionResponse object
        """
        execution_id = execution_response.get("QueryExecutionId")
        execution_res_location = output_loc + execution_id + ".txt"

        return ExecutionResponse(execution_id=execution_id, execution_res_loc=execution_res_location)

    def execute_query(self, query: str) -> ExecutionResponse:
        """Executes an athena query.

        Args:
            - query: a sql query to be executed in athena

        Returns:
            - resp: ExecutionResponse object parsed from AWS Athena API response
        """
        try:
            resp = self.client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": self.database},
                ResultConfiguration={"OutputLocation": self.output_loc},
            )
        except ClientError as e:
            raise e
        else:
            return self._process_execution_response(execution_response=resp)

    def succeeded(self, execution_response: ExecutionResponse, query: str) -> dict:
        """Handler for query succeeded.

        Args:
            - execution_response: A ExecutionResponse
            - query: a valid query string

        Returns:
            - dictionary returned from self.results function
        """
        logger.info(
            {
                "response": f"Query '{query}' succeeded",
                "status": "success",
                "execution_id": execution_response["execution_id"],
            }
        )
        return self.results(execution_response)

    def cancelled(self, execution_response: ExecutionResponse, query: str):
        """Handler for query cancelled.

        Args:
            - execution_response: A ExecutionResponse
            - query: a valid query string

        Returns:
            - Exception
        """
        logger.info(
            {
                "response": f"Query {query} cancelled",
                "status": "failed",
                "execution_id": execution_response["execution_id"],
            }
        )
        # Set execution id because f strings don't like dict lookups
        execution_id = execution_response["execution_id"]

        raise Exception(f"Query cancelled. Query Execution ID: {execution_id}")

    def failed(self, execution_response: ExecutionResponse, query: str):
        """Handler for query failed.

        Args:
            - execution_response: A ExecutionResponse
            - query: a valid query string

        Returns:
            - Exception
        """
        logger.info(
            {
                "response": f"Query {query} failed",
                "status": "failed",
                "execution_id": execution_response["execution_id"],
            }
        )
        # Set execution id because f strings don't like dict lookups
        execution_id = execution_response["execution_id"]

        raise Exception(f"Query failed. Query Execution ID: {execution_id}")

    def execute_and_wait(self, query: str) -> dict:
        """Execute a query and wait for completion.

        Args:
            - query: a valid query string

        Returns:
            - Dict
        """
        execution_response = self.execute_query(query=query)
        status = self.wait_for_completion(execution_response=execution_response)

        # Handle the status of the query dynamically
        handler = getattr(self, status.lower(), None)
        return handler(execution_response=execution_response, query=query)


class TablePartitions:
    """Object representing partitions of a table.

    Methods allow for getting, checking if exists, and adding partitions.
    """

    def __init__(self, athena_client, table) -> None:
        self.athena_client = athena_client
        self.table = table

    def _build_partition_query(
        self, bucket_loc: str, new_partition: dict, action_string: str = "IF NOT EXISTS"
    ) -> str:
        """Builds a query to add a new partition.

        Args:
            - bucket_loc: A valid path in an S3 bucket
            - new_partition: list of partition items (eg: {'region': 'ap-south-1', 'year': '2020', 'month': '04', 'day': '01'}) # noqa

        Returns:
            - str: a SQL query as a string to add a new partition from a valid path
        """
        n = len(new_partition) - 1
        i = 0

        location_statement = f"s3://{bucket_loc}/"

        partition = ""

        for k, v in new_partition.items():
            if i == n:
                partition += f"{k}='{v}'"
            else:
                partition += f"{k}='{v}',"
                i += 1

            location_statement += v + "/"

        query = f"ALTER TABLE {self.table} ADD {action_string} PARTITION ({partition}) LOCATION '{location_statement}'"  # noqa

        return query

    def add_partition(self, bucket_loc: str, new_partition: dict) -> dict:
        """Add a partition to the table.

        Args:
            - bucket_loc: A valid path in an S3 bucket
            - new_partition: list of partition items (eg: {'region': 'ap-south-1', 'year': '2020', 'month': '04', 'day': '01'}) # noqa

        Returns:
            - dict: results from self.athena_client.execute_and_wait()
        """
        query = self._build_partition_query(bucket_loc=bucket_loc, new_partition=new_partition)

        return self.athena_client.execute_and_wait(query=query)

    def _get_partition_query(self, region: str, year: str, month: str, day: str) -> str:
        """Builds a query to check if a particular partition exists already.

        Args:
            - region: an AWS region
            - year: a year as a string
            - month: a month (eg: 04) as a string
            - day: a day of a month (eg: 01) as a string:

        Returns:
            - str: A valid query for checking if a partition exists
        """

        return f"""SELECT kv['region'] AS region, kv['year'] AS year, kv['month'] AS month, kv['day'] AS day
        FROM
            (SELECT partition_number,
                map_agg(partition_key,
                partition_value) kv
            FROM information_schema.__internal_partitions__
            WHERE table_schema = '{self.athena_client.database}'
                    AND table_name = '{self.table}'
            GROUP BY  partition_number)
        WHERE kv['region'] = '{region}'
            AND kv['year'] = '{year}'
            AND kv['month'] = '{month}'
            AND kv['day'] = '{day}'"""

    def _check_partition_result(self, results: dict) -> bool:
        """Checks if the query returns more than the header row.

        This is a safe binary operation because the first result will always be a header row
        All sequential results will be matched data, so we can assume if len(results["Rows"] > 1)
        then the partition exists

        Args:
            - results: dictionary of results from athena

        Returns:
            - bool: True if more than header row is returned, False if not (this means partition exists)
        """
        # This query is very specific so we can count on the paginator only returning a single page
        row_length = len(results[0]["Rows"])

        # If l == 1, only the header row was returned, so the partition does not exist
        if row_length == 1:
            return False

        return True

    def check_for_partition(self, new_partition: dict) -> bool:
        """Check if a new partition is already in the partition map.

        Args:
            - new_partition: a list determined from s3 event input (ex: {'region': 'ap-south-1', 'year': '2020', 'month': '04', 'day': '01'}) # noqa

        Returns:
            - bool: True if partition exists, False if not
        """
        query = self._get_partition_query(**new_partition)

        # Wait for query to complete, return results
        partition_results = self.athena_client.execute_and_wait(query=query)

        # Return True or False from _check_partition_result
        return self._check_partition_result(partition_results)


class Event:
    """Object representing an event from an EventBridge (cloudwatch) timed event."""

    def __init__(self, event: dict) -> None:
        for k, v in event.items():
            setattr(self, k.replace("-", "_"), v)

    @property
    def event_month(self) -> str:
        month = self._convert_to_datetime(getattr(self, "time", None)).month
        return str(month) if month >= 10 else str(month).zfill(2)

    @property
    def event_year(self) -> str:
        return str(self._convert_to_datetime(getattr(self, "time", None)).year)

    @property
    def event_day(self) -> str:
        day = self._convert_to_datetime(getattr(self, "time", None)).day
        return str(day) if day >= 10 else str(day).zfill(2)

    def _convert_to_datetime(self, timestamp: str) -> Optional[datetime]:
        """Receives time in %Y-%m-%dT%H:%M:%S.%fZ format and returns float of seconds since epoch"""
        if timestamp:
            time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%fZ")

            return time

        return None


# Instantiate the client outside of the scope of the handler so it gets cached
athena = Athena(database=database, output_loc=output_loc)


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


def lambda_handler(event, context) -> bool:
    """Event handler that handles S3 Put events for new Cloudtrail Logs objects.

    Args:
        - event: Event coming from S3
        - context: context of the lambda execution

    Returns:
        - bool
    """
    bucket_loc = f"{bucket}/AWSLogs/{account_id}/CloudTrail/"
    event = Event(event=event)

    helper = S3Helper(bucket=bucket, account_id=account_id)

    # Create the table client
    table = TablePartitions(athena_client=athena, table=table_name)

    for region in helper.regions:
        new_partition = {
            "region": region,
            "year": event.event_year,
            "month": event.event_month,
            "day": event.event_day,
        }

        # Check the table, add or do not
        if table.check_for_partition(new_partition):
            logger.info({"response": "partition_exists", "status": "passing"})
        else:
            logger.info({"response": "partition_does_not_exist", "status": "creating"})
            table.add_partition(bucket_loc, new_partition)
    return True
