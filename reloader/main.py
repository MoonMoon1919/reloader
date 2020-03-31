"""
Automatically load newly created S3 prefixes into Athena.

- Retrieve list of pre-existing partitions
- Load into object
- Check if prefix exists in existing list
- Run add partition query
- Wait for complete
- Check status
- Check partition list for new partition in partition list
"""

import json
import logging
from time import sleep
from typing import TypedDict
import sys

import boto3
from botocore.exceptions import ClientError

client = boto3.client("athena")
paginator = client.get_paginator("get_query_results")

# TODO: Move to environment variables before shipping to prod
database = "default"
table = "cloudtrail_logs"
output_loc = "s3://mmoon-athena-results/cloudtrail/add_partition/"

# Create the logger
logger = logging.getLogger("peyton")
logger.setLevel(logging.INFO)
default_handler = logging.StreamHandler(sys.stderr)
default_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(default_handler)


class ExecutionResponse(TypedDict):
    execution_id: str
    execution_res_loc: str


class Athena:
    def __init__(self, database: str, output_loc: str):
        self.database = database
        self.output_loc = output_loc

    def results(self, execution_response: ExecutionResponse) -> dict:
        try:
            resp = paginator.paginate(
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
                resp = client.get_query_execution(
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
            resp = client.start_query_execution(
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
        return self.results(execution_data)


def partitions(results):
    partitions = []

    for result in results:
        for row in result["Rows"]:
            partion = row["Data"][0]["VarCharValue"]
            splitter = partion.split("/")

            data = {}

            for item in splitter:
                k, v = item.split("=")

                data[k] = v

            partitions.append(data)

    return partitions


def get_partitions():
    query = f"SHOW PARTITIONS {table}"

    # Partition result sets
    partition_results = athena.execute_and_wait(query=query)

    # Get list of partitions
    partition_list = partitions(partition_results)

    return partition_list


athena = Athena(database=database, output_loc=output_loc)

print(get_partitions())
