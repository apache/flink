/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.test.async;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/** Example application that tests AsyncScalarFunction with Table API. */
public class AsyncScalarFunctionExample {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncScalarFunctionExample.class);

    /**
     * Sets up and executes the Flink processing pipeline.
     *
     * @return TableResult from the execution
     */
    public TableResult execute() throws Exception {
        LOG.info("Starting AsyncScalarFunctionExample");

        // Set up Table Environment
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Register source table
        createSourceTable(tableEnv);

        // Register sink table
        createSinkTable(tableEnv);

        // Register the lookup function
        registerLookupFunction(tableEnv);

        // Create and execute the transformation using Table API
        TableResult result = processProducts(tableEnv);

        LOG.info("Executing Flink job");

        return result;
    }

    /** Creates the source table using datagen connector. */
    protected void createSourceTable(TableEnvironment tableEnv) {
        final Schema schema =
                Schema.newBuilder()
                        .column("product_id", DataTypes.STRING())
                        .column("price", DataTypes.DECIMAL(10, 2))
                        .column("quantity", DataTypes.INT())
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .watermark("ts", "ts - INTERVAL '5' SECOND")
                        .build();

        // Create a temporary mock products table
        tableEnv.createTemporaryTable(
                "products",
                TableDescriptor.forConnector("datagen")
                        .schema(schema)
                        .option("number-of-rows", "10")
                        .option("fields.product_id.kind", "sequence")
                        .option("fields.product_id.start", "1")
                        .option("fields.product_id.end", "10")
                        .option("fields.price.min", "10.00")
                        .option("fields.price.max", "100.00")
                        .option("fields.quantity.min", "1")
                        .option("fields.quantity.max", "10")
                        .build());

        LOG.info("Source table 'products' created");
    }

    /** Creates the sink table using blackhole connector. */
    protected void createSinkTable(TableEnvironment tableEnv) {
        tableEnv.createTemporaryTable(
                "enriched_products",
                TableDescriptor.forConnector("blackhole")
                        .schema(
                                Schema.newBuilder()
                                        .column("product_id", DataTypes.STRING())
                                        .column("price", DataTypes.DECIMAL(10, 2))
                                        .column("quantity", DataTypes.INT())
                                        .column("timestamp", DataTypes.TIMESTAMP(3))
                                        .column("name", DataTypes.STRING())
                                        .build())
                        .build());

        LOG.info("Sink table 'enriched_products' created");
    }

    /** Registers the async lookup function. */
    protected void registerLookupFunction(TableEnvironment tableEnv) {
        tableEnv.createTemporaryFunction("lookup_name", new MockAsyncLookupFunction("name"));
        LOG.info("Registered async lookup function");
    }

    /**
     * Processes products data using Table API with async lookups. - Performs async lookups -
     * Enriches the data with product details - Inserts into the sink table
     *
     * @return TableResult from the execution
     */
    protected TableResult processProducts(TableEnvironment tableEnv) {
        Table products = tableEnv.from("products");
        Table enrichedProducts =
                products.select(
                        $("product_id"),
                        $("price"),
                        $("quantity"),
                        $("ts").as("timestamp"),
                        call("lookup_name", $("product_id")).as("name"));

        // Execute the query - will fail with NoClassDefFoundError for StringSubstitutor
        TableResult result = enrichedProducts.executeInsert("enriched_products");

        LOG.info("Product enrichment transformation created");

        return result;
    }

    /** Application entry point. */
    public static void main(String[] args) throws Exception {
        AsyncScalarFunctionExample example = new AsyncScalarFunctionExample();

        try {
            TableResult result = example.execute();

            LOG.info("Job completed successfully");
            result.await();
        } catch (Throwable t) {
            // Log the exception chain to find the root cause
            LOG.error("Job failed with exception", t);
        }
    }

    /** Mock implementation of an AsyncScalarFunction that simulates async lookups. */
    public static class MockAsyncLookupFunction extends AsyncScalarFunction {
        private static final long serialVersionUID = 1L;

        private final String fieldType;
        private transient ExecutorService executorService;

        public MockAsyncLookupFunction(String fieldType) {
            this.fieldType = fieldType;
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            executorService =
                    Executors.newFixedThreadPool(2, new ExecutorThreadFactory("mock-lookup"));
        }

        // The correct implementation for AsyncScalarFunction.eval()
        // - Must return void
        // - Must take a CompletableFuture as the first parameter
        public void eval(CompletableFuture<String> resultFuture, String key) {
            executorService.submit(
                    () -> {
                        resultFuture.complete("Product " + key);
                    });
        }

        @Override
        public void close() throws Exception {
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            }
        }
    }
}
