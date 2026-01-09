################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""
Example demonstrating the usage of Python Async Scalar Functions in PyFlink.

This example shows how to use AsyncScalarFunction for asynchronous operations
such as database lookups, REST API calls, or other I/O-bound operations that
would benefit from async execution.
"""

import asyncio
import logging
import random
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.udf import AsyncScalarFunction, udf


# Example 1: Class-based Async Scalar Function
class AsyncDatabaseLookup(AsyncScalarFunction):
    """
    Simulates an async database lookup operation.
    In real scenarios, this would interact with an async database client.
    """

    def open(self, function_context):
        # Initialize resources (e.g., database connection pool)
        self.cache = {}

    async def eval(self, key: str) -> str:
        # Check cache first
        if key in self.cache:
            return self.cache[key]

        # Simulate async database query
        await asyncio.sleep(0.1)  # Simulate I/O delay

        # Generate result
        value = f"db_value_for_{key}"
        self.cache[key] = value
        return value

    def close(self):
        # Clean up resources
        self.cache.clear()


# Example 2: Decorator-based Async Scalar Function
@udf(result_type=DataTypes.STRING())
async def async_api_call(product_id: str) -> str:
    """
    Simulates an async REST API call to fetch product information.
    """
    # Simulate API call delay
    await asyncio.sleep(0.05)

    # Simulate API response
    price = random.randint(10, 1000)
    return f"Product {product_id}: ${price}"


def async_scalar_function_example():
    # Create table environment
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    # Register the async scalar functions
    t_env.create_temporary_function("async_db_lookup",
                                    udf(AsyncDatabaseLookup(), result_type=DataTypes.STRING()))
    t_env.create_temporary_function("async_api_call", async_api_call)

    # Create a source table
    t_env.execute_sql("""
        CREATE TABLE source_table (
            user_id STRING,
            product_id STRING,
            score INT
        ) WITH (
            'connector' = 'datagen',
            'number-of-rows' = '10',
            'fields.user_id.length' = '1',
            'fields.product_id.length' = '1',
            'fields.score.min' = '50',
            'fields.score.max' = '100'
        )
    """)

    print("Created source table with datagen connector\n")

    # Use async scalar functions in SQL queries
    print("Example Query 1: Using async_db_lookup")
    t_env.sql_query("""
        SELECT
            user_id,
            async_db_lookup(user_id) as user_info
        FROM source_table
    """).execute().print()

    print("Example Query 2: Using async_api_call")
    t_env.sql_query("""
        SELECT
            product_id,
            async_api_call(product_id) as product_info
        FROM source_table
    """).execute().print()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    async_scalar_function_example()
