"""
Automatically load newly created S3 prefixes into Athena.
"""

# Import from std lib
from functools import cached_property
import logging
import os
from time import sleep
from typing import TypedDict
import sys

# Import from
import boto3
from botocore.exceptions import ClientError

# Move this to OS environ or SSM parameter store
bucket = os.environ.get("BUCKET")
log_location = os.environ.get("LOG_LOCATION")
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


def generator(data):
    """Generator."""
    for row in data:
        yield row


def traverse_dict(data, value, data_type):
    """Traverse a dict all the way to the bottom."""
    if data.get(value):
        data = data[value]
    else:
        data[value] = data_type
        data = data[value]

    return data


class ExecutionResponse(TypedDict):
    """Data structure for Athena execution response."""

    execution_id: str
    execution_res_loc: str


class Athena:
    def __init__(self, database: str, output_loc: str) -> None:
        self.database = database
        self.output_loc = output_loc

        self.client = boto3.client("athena")
        self.paginator = self.client.get_paginator("get_query_results")

    def results(self, execution_response: ExecutionResponse) -> dict:
        try:
            resp = self.paginator.paginate(
                QueryExecutionId=execution_response["execution_id"]
            )
        except ClientError as e:
            raise e
        else:
            results = [result["ResultSet"] for result in resp]
        finally:
            return results

    def wait_for_completion(self, execution_response: ExecutionResponse) -> str:
        status = "RUNNING"

        while status not in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            try:
                resp = self.client.get_query_execution(
                    QueryExecutionId=execution_response["execution_id"]
                )
            except ClientError as e:
                raise e
            else:
                status = resp["QueryExecution"]["Status"]["State"]
                sleep(0.25)

        return status

    def process_execution_response(self, execution_response: dict) -> ExecutionResponse:
        execution_id = execution_response.get("QueryExecutionId")
        execution_res_location = output_loc + execution_id + ".txt"

        return ExecutionResponse(
            execution_id=execution_id, execution_res_loc=execution_res_location
        )

    def execute_query(self, query: str) -> dict:
        """Executes an athena query.

        Args:
            - query: a sql query to be executed in athena
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
            return resp

    def succeeded(self, execution_response, query) -> dict:
        """Handler for query succeeded.

        Args:
            - execution_response: A ExecutionResponse
            - query: a valid query string
        """
        logger.info(
            {
                "response": f"Query '{query}' succeeded",
                "status": "success",
                "execution_id": execution_response["execution_id"],
            }
        )
        return self.results(execution_response)

    def cancelled(self, execution_response, query) -> bool:
        """Handler for query cancelled.

        Args:
            - execution_response: A ExecutionResponse
            - query: a valid query string
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

    def failed(self, execution_response, query) -> bool:
        """Handler for query failed.

        Args:
            - execution_response: A ExecutionResponse
            - query: a valid query string
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
        """
        execution_response = self.execute_query(query=query)
        execution_response = self.process_execution_response(
            execution_response=execution_response
        )
        status = self.wait_for_completion(execution_response=execution_response)

        # Handle the status of the query dynamically
        handler = getattr(self, status.lower(), None)
        return handler(execution_response=execution_response, query=query)


class TablePartitions:
    """Object representing partitions of a table.

    Methods allow for getting, checking if exists, and adding partitions.
    """

    def __init__(self, athena_client, table, schema) -> None:
        self.athena_client = athena_client
        self.table = table
        self.schema = schema

    def _build_add_partition_query(self, bucket_loc, new_partition):
        """Builds a query to add a new partition."""
        n = len(new_partition) - 1

        location_statement = f"s3://{bucket_loc}/"

        partition = ""

        for i, d in enumerate(new_partition):
            if i == n:
                partition += f"{self.schema[i]}='{d}'"
            else:
                partition += f"{self.schema[i]}='{d}',"

            location_statement += d + "/"

        query = f"ALTER TABLE {self.table} ADD PARTITION ({partition}) LOCATION '{location_statement}'"

        return query

    def add_partition(self, bucket_loc: str, new_partition: str):
        """Add a partition to the table."""
        query = self._build_add_partition_query(
            bucket_loc=bucket_loc, new_partition=new_partition
        )

        return self.athena_client.execute_and_wait(query=query)

    def check_for_partition(self, new_partition: list) -> bool:
        """Check if a new partition is already in the partition map."""
        n = len(new_partition) - 1
        partition_map = self.partitions

        for i, d in enumerate(new_partition):
            if i == n:
                if d in partition_map:
                    return True
                else:
                    return False
            elif i == (n - 1):
                partition_map = traverse_dict(partition_map, d, [])
            else:
                partition_map = traverse_dict(partition_map, d, {})

    def _get_partitions(self, results) -> dict:
        """Create a map of partitions in the table."""
        partitions = {}

        for result in results:
            for row in generator(result["Rows"]):
                data = row["Data"][0]["VarCharValue"]
                splitter = data.split("/")
                splitter = [s.split("=")[1] for s in splitter]
                n = len(splitter) - 1

                data = partitions

                for i, d in enumerate(splitter):
                    if i == n:
                        data.append(d)
                    elif i == (n - 1):
                        data = traverse_dict(data, d, [])
                    else:
                        data = traverse_dict(data, d, {})

        return partitions

    @cached_property
    def partitions(self) -> dict:
        """Map of partitions."""
        query = f"SHOW PARTITIONS {self.table}"

        # Partition result sets
        partition_results = self.athena_client.execute_and_wait(query=query)

        # Get list of partitions
        partition_map = self._get_partitions(partition_results)

        return partition_map


class Event:
    """Object representing an event from S3."""

    def __init__(self, event) -> None:
        for k, v in event.items():
            if isinstance(v, dict):
                nu_v = Event(v)
                setattr(self, k, nu_v)
            else:
                setattr(self, k, v)


# Instantiate the client outside of the scope of the handler so it gets cached
athena = Athena(database=database, output_loc=output_loc)


def lambda_handler(event, context):
    """Event handler that handles S3 Put events for new Cloudtrail Logs objects.

    Args:
        - event: Event coming from S3
        - context: context of the lambda execution
    """
    # TODO: Create this dynamically..
    schema = {0: "region", 1: "year", 2: "month", 3: "day"}

    # Keep below
    ignore_path = f"{log_location}/{account_id}/Cloudtrail"
    bucket_loc = f"{bucket}/{log_location}/{account_id}/CloudTrail/"

    # Create the table client
    table = TablePartitions(athena_client=athena, table=table_name, schema=schema)

    # Load the event into an event object
    event = Event(event["Records"][0])
    new_partition = event.s3.object.key.split("/")[3:-1]

    # Check the table, add or do not
    if table.check_for_partition(new_partition):
        logger.info({"response": "partition_exists", "status": "passing"})
    else:
        logger.info({"response": "partition_does_not_exist", "status": "creating"})
        table.add_partition(bucket_loc, new_partition)

    return True
