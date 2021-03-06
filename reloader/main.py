"""
Automatically load newly created S3 prefixes into Athena.
"""

# Import from std lib
from datetime import datetime, timedelta
from functools import cached_property
import json
import logging
import os
from time import sleep
from typing import TypedDict, Optional, Any

# Import from
import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

# Retrieve env vars
bucket = os.environ.get("BUCKET")
account_id = os.environ.get("ACCOUNT_ID")
database = os.environ.get("DATABASE")
table_name = os.environ.get("TABLE_NAME")
output_loc = os.environ.get("OUTPUT_LOC")

# Clear the default lambda logger so we can output json without INFO info and asctime
root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("athena-cloudtrail-reloader")


class Encoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, set):
            return tuple(o)
        elif isinstance(o, str):
            return o.encode("unicode_escape").decode("ascii")
        elif isinstance(o, datetime):
            return o.isoformat()
        return super(Encoder, self).default(o)


class JsonLog(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __str__(self):
        return Encoder().encode(self.kwargs)


class ExecutionResponse(TypedDict):
    """Data structure for Athena execution response."""

    execution_id: str
    execution_res_loc: str


class Athena:
    """Class for interacting with AWS Athena."""

    def __init__(self, database: str = None, output_loc: str = None) -> None:
        self.database = database
        self.output_loc = output_loc

        self.client = boto3.client("athena")
        self.paginator = self.client.get_paginator("get_query_results")

    def results(self, execution_response: ExecutionResponse) -> list:
        try:
            resp = self.paginator.paginate(QueryExecutionId=execution_response["execution_id"])
        except ClientError as e:
            raise e
        else:
            results = [result["ResultSet"] for result in resp]
        finally:
            return results

    def wait_for_completion(self, execution_response: ExecutionResponse) -> Optional[Any]:
        """Execute an Athena query and wait until it has succeeded, failed, or been cancelled.

        Args:
            - execution_response: an ExecutionResponse containing the execution ID

        Returns:
            - str: from ["SUCCEEDED", "FAILED", "CANCELLED"]
        """
        status = "RUNNING"
        resp = None

        while status not in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            try:
                resp = self.client.get_query_execution(QueryExecutionId=execution_response["execution_id"])
            except ClientError as e:
                raise e
            else:
                status = resp["QueryExecution"]["Status"]["State"]
                sleep(0.25)

        return resp

    def _process_execution_response(self, execution_response: dict) -> ExecutionResponse:
        """Takes the execution response from Athena's start_query_execution call
        and converts it to a ExecutionResponse object

        Args:
            - execution_response: The execution response from start_query_execution

        Returns:
            - ExecutionResponse object
        """
        execution_id = execution_response.get("QueryExecutionId")

        if execution_id:
            execution_res_location = self.output_loc + execution_id + ".txt"
            return ExecutionResponse(execution_id=execution_id, execution_res_loc=execution_res_location)

        return ExecutionResponse(execution_id="", execution_res_loc="")

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

    def succeeded(
        self,
        execution_response: ExecutionResponse,
        query: str,
        query_start: datetime,
        query_end: datetime,
        current_time: datetime,
        x_amzn_request_id: str,
    ) -> list:
        """Handler for query succeeded.

        Args:
            - execution_response: A ExecutionResponse
            - query: a valid query string

        Returns:
            - dictionary returned from self.results function
        """
        logger.info(
            JsonLog(
                **{
                    "response": "Query succeeded",
                    "query": query,
                    "status": "success",
                    "execution_id": execution_response["execution_id"],
                    "query_start": query_start,
                    "query_end": query_end,
                    "current_time": current_time,
                    "x_amzn_request_id": x_amzn_request_id,
                }
            )
        )
        return self.results(execution_response)

    def cancelled(
        self,
        execution_response: ExecutionResponse,
        query: str,
        query_start: datetime,
        query_end: datetime,
        current_time: datetime,
        x_amzn_request_id: str,
    ):
        """Handler for query cancelled.

        Args:
            - execution_response: A ExecutionResponse
            - query: a valid query string

        Returns:
            - Exception
        """
        logger.info(
            JsonLog(
                **{
                    "response": "Query cancelled",
                    "query": query,
                    "status": "failed",
                    "execution_id": execution_response["execution_id"],
                    "query_start": query_start,
                    "query_end": query_end,
                    "current_time": current_time,
                    "x_amzn_request_id": x_amzn_request_id,
                }
            )
        )
        # Set execution id because f strings don't like dict lookups
        execution_id = execution_response["execution_id"]

        raise Exception(f"Query cancelled. Query Execution ID: {execution_id}")

    def failed(
        self,
        execution_response: ExecutionResponse,
        query: str,
        query_start: datetime,
        query_end: datetime,
        current_time: datetime,
        x_amzn_request_id: str,
    ):
        """Handler for query failed.

        Args:
            - execution_response: A ExecutionResponse
            - query: a valid query string

        Returns:
            - Exception
        """
        logger.info(
            JsonLog(
                **{
                    "response": "Query failed",
                    "query": query,
                    "status": "failed",
                    "execution_id": execution_response["execution_id"],
                    "query_start": query_start,
                    "query_end": query_end,
                    "current_time": current_time,
                    "x_amzn_request_id": x_amzn_request_id,
                }
            )
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
        resp = self.wait_for_completion(execution_response=execution_response)

        if resp is not None:
            # Retrieve necessary data from the resp
            status = resp.get("QueryExecution", {}).get("Status", {}).get("State", None)
            query_start = resp.get("QueryExecution", {}).get("Status", {}).get("SubmissionDateTime")
            query_end = resp.get("QueryExecution", {}).get("Status", {}).get("CompletionDateTime")
            x_amzn_request_id = resp.get("ResponseMetadata", {}).get("RequestId")

            # Get current time for the log time
            current_time = datetime.now()

            # Handle the status of the query dynamically
            if status:
                handler = getattr(self, status.lower(), None)
                return handler(
                    execution_response=execution_response,
                    query=query,
                    query_start=query_start,
                    query_end=query_end,
                    x_amzn_request_id=x_amzn_request_id,
                    current_time=current_time,
                )

        return {}


class TablePartition:
    """Object representing partitions of a table.

    Methods allow for getting, checking if exists, and adding partitions.
    """

    def __init__(self, athena_client, table) -> None:
        self.athena_client = athena_client
        self.table = table

    def _build_partition_query(self, bucket_loc: str, partition: dict, action_string: str) -> str:
        """Builds a query to add add or drop a partition.

        Args:
            - bucket_loc: A valid path in an S3 bucket
            - partition: list of partition items (eg: {'region': 'ap-south-1', 'year': '2020', 'month': '04', 'day': '01'}) # noqa
            - action_str: 'ADD' or 'DROP', a string that describes an action to perform

        Returns:
            - str: a SQL query as a string to add a new partition from a valid path
        """
        if action_string == "ADD":
            action_conditional = "IF NOT EXISTS"
        elif action_string == "DROP":
            action_conditional = "IF EXISTS"
        else:
            raise ValueError("action_string must be ADD or DROP")

        n = len(partition) - 1
        i = 0

        location_statement = f"s3://{bucket_loc}/"

        partition_str = ""

        for k, v in partition.items():
            if i == n:
                partition_str += f"{k}='{v}'"
            else:
                partition_str += f"{k}='{v}',"
                i += 1

            location_statement += v + "/"

        query = f"ALTER TABLE {self.table} {action_string} {action_conditional} PARTITION ({partition_str})"  # noqa

        if action_string == "ADD":
            query += f" LOCATION '{location_statement}'"

        return query

    def add_partition(self, bucket_loc: str, partition: dict) -> dict:
        """Add a partition to the table.

        Args:
            - bucket_loc: A valid path in an S3 bucket
            - partition: list of partition items (eg: {'region': 'ap-south-1', 'year': '2020', 'month': '04', 'day': '01'}) # noqa

        Returns:
            - dict: results from self.athena_client.execute_and_wait()
        """
        query = self._build_partition_query(bucket_loc=bucket_loc, partition=partition, action_string="ADD")

        return self.athena_client.execute_and_wait(query=query)

    def drop_partition(self, bucket_loc: str, partition: dict) -> dict:
        """Drop a partition to the table.

        Args:
            - bucket_loc: A valid path in an S3 bucket
            - partition: list of partition items (eg: {'region': 'ap-south-1', 'year': '2020', 'month': '04', 'day': '01'}) # noqa

        Returns:
            - dict: results from self.athena_client.execute_and_wait()
        """
        query = self._build_partition_query(bucket_loc=bucket_loc, partition=partition, action_string="DROP")

        return self.athena_client.execute_and_wait(query=query)


class Event:
    """Object representing an event from an EventBridge (cloudwatch) timed event."""

    def __init__(self, event: dict) -> None:
        for k, v in event.items():
            if k == "time":
                v = self._convert_to_datetime(timestamp=v)

            setattr(self, k.replace("-", "_"), v)

    @property
    def event_month(self) -> str:
        month = self.time.month  # type: ignore
        return str(month).zfill(2)

    @property
    def event_year(self) -> str:
        return str(self.time.year)  # type: ignore

    @property
    def event_day(self) -> str:
        day = self.time.day  # type: ignore
        return str(day).zfill(2)

    def _convert_to_datetime(self, timestamp: str) -> Optional[datetime]:
        """Receives time in %Y-%m-%dT%H:%M:%S.%fZ format and returns float of seconds since epoch"""
        if timestamp:
            time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%fZ")

            return time

        return None


# Instantiate the client outside of the scope of the handler so it gets cached
athena = Athena(database=database, output_loc=output_loc)


class S3Helper:
    def __init__(self, bucket: str = None, account_id: str = None):
        self._bucket = bucket
        self._account_id = account_id
        self._client = boto3.client("s3")

    @cached_property
    def regions(self):
        return self._get_regions()

    @cached_property
    def expiration_after_days(self):
        return self._get_bucket_lifecycle_expiration()

    def _get_bucket_lifecycle_expiration(self) -> Optional[int]:
        """Given a bucket, retrieve the amount of days before expiration."""
        try:
            resp = self._client.get_bucket_lifecycle_configuration(Bucket=self._bucket)
        except ClientError as error:
            logger.info(JsonLog(error=error.response))
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
            logger.info(JsonLog(error=error.response))
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
                    logger.info(JsonLog(error="unable to find region"))
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
    bucket_loc = f"{bucket}/AWSLogs/{account_id}/CloudTrail"
    event = Event(event=event)

    helper = S3Helper(bucket=bucket, account_id=account_id)

    # Create the table client
    table = TablePartition(athena_client=athena, table=table_name)

    for region in helper.regions:
        new_partition = {
            "region": region,
            "year": event.event_year,
            "month": event.event_month,
            "day": event.event_day,
        }

        # Add the partition
        table.add_partition(bucket_loc=bucket_loc, partition=new_partition)

        if helper.expiration_after_days:
            # N days ago
            drop_after_expire_date = event.time - timedelta(days=helper.expiration_after_days)

            old_partition = {
                "region": region,
                "year": str(drop_after_expire_date.year),
                "month": str(drop_after_expire_date.month).zfill(2),
                "day": str(drop_after_expire_date.day).zfill(2),
            }

            # Drop old partitions that don't contain data
            table.drop_partition(bucket_loc=bucket_loc, partition=old_partition)

    return True
