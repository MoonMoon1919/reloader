"""
Automatically load newly created S3 prefixes into Athena.
"""

from functools import cached_property
import logging
from time import sleep
from typing import TypedDict
import sys

import boto3
from botocore.exceptions import ClientError

# TODO: Move to environment variables before shipping to prod
database = "default"
table = "cloudtrail_logs"
output_loc = "s3://mmoon-athena-results/cloudtrail/add_partition/"
schema = {0: "region", 1: "year", 2: "month", 3: "day"}

# Create the logger
logger = logging.getLogger("peyton")
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

    def wait_for_completion(self, execution_data: ExecutionResponse) -> str:
        status = "RUNNING"

        while status not in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            try:
                resp = self.client.get_query_execution(
                    QueryExecutionId=execution_data["execution_id"]
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

    def execute_and_wait(self, query: str) -> dict:
        execution_response = self.execute_query(query=query)
        execution_data = self.process_execution_response(
            execution_response=execution_response
        )
        status = self.wait_for_completion(execution_data=execution_data)
        # TODO: Add handling for statuses
        return self.results(execution_data)


class TablePartitions:
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

    @property
    def partitions(self) -> dict:
        """Map of partitions."""
        query = f"SHOW PARTITIONS {self.table}"

        # Partition result sets
        partition_results = self.athena_client.execute_and_wait(query=query)

        # Get list of partitions
        partition_map = self._get_partitions(partition_results)

        return partition_map


athena = Athena(database=database, output_loc=output_loc)
table = TablePartitions(athena_client=athena, table=table, schema=schema)

new_partition = ["us-east-1", "2020", "04", "07"]

# TODO: Add event parsing logic
# Build the bucket location (will be done via event later)
bucket = "mmoon-cloudtrail"
log_location = "AWSLogs"
account_id = "123456789012"
bucket_loc = f"{bucket}/{log_location}/{account_id}/CloudTrail/"

if table.check_for_partition(new_partition):
    print("Partition exists, passing!")
else:
    table.add_partition(bucket_loc, new_partition)
