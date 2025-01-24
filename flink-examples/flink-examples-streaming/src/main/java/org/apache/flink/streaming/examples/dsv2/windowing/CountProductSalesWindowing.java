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

package org.apache.flink.streaming.examples.dsv2.windowing;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.api.connector.dsv2.WrappedSource;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.builtin.BuiltinFuncs;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.window.context.OneInputWindowContext;
import org.apache.flink.datastream.api.extension.window.function.OneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.strategy.WindowStrategy;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.ParameterTool;

import java.time.Duration;
import java.util.Objects;

/**
 * Example illustrating how to use Window to count the sales of each product in each hour by
 * DataStream API V2.
 *
 * <p>The input is a list of orders, each order is a consist of productId and orderTimestamp.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li><code>--output &lt;path&gt;</code>The output directory where the Job will write the
 *       results. If no output path is provided, the Job will print the results to <code>stdout
 *       </code>.
 * </ul>
 *
 * <p>This example shows how to:
 *
 * <ul>
 *   <li>Usage of Window extension in DataStream API V2
 * </ul>
 *
 * <p>Please note that if you intend to run this example in an IDE, you must first add the following
 * VM options: "--add-opens=java.base/java.util=ALL-UNNAMED". This is necessary because the module
 * system in JDK 17+ restricts some reflection operations.
 *
 * <p>Please note that the DataStream API V2 is a new set of APIs, to gradually replace the original
 * DataStream API. It is currently in the experimental stage and is not fully available for
 * production.
 */
public class CountProductSalesWindowing {

    /** Pojo class for order. */
    public static class Order {
        public long productId;
        public long timestamp;
    }

    /** The {@link ProductSales} class represents the count of product sales within one hour. */
    public static class ProductSales {
        public long productId;
        public long startTime;
        public long salesQuantity;

        public ProductSales(long productId, long startTime, long salesQuantity) {
            this.productId = productId;
            this.startTime = startTime;
            this.salesQuantity = salesQuantity;
        }

        @Override
        public String toString() {
            return String.format("%d,%d,%d", this.productId, this.startTime, this.salesQuantity);
        }
    }

    public static void main(String[] args) throws Exception {
        // parse the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final boolean fileOutput = params.has("output");

        // Get the execution environment instance. This is the main entrypoint
        // to building a Flink application.
        final ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        // create order source stream
        // We provide CSV files to represent the sample input data, which can be found in the
        // resources directory.
        String inputFilePath =
                Objects.requireNonNull(
                                CountProductSalesWindowing.class
                                        .getClassLoader()
                                        .getResource(
                                                "datas/dsv2/windowing/CountProductSalesWindowingOrders.csv"))
                        .getPath();
        // create a FileSource with CSV format to read the input data
        FileSource<Order> fileSource =
                FileSource.forRecordStreamFormat(
                                CsvReaderFormat.forPojo(Order.class), new Path(inputFilePath))
                        .build();
        NonKeyedPartitionStream<Order> orders =
                env.fromSource(new WrappedSource<>(fileSource), "order source");

        // extract and propagate event time from order
        NonKeyedPartitionStream<Order> orderStream =
                orders.process(
                        EventTimeExtension.<Order>newWatermarkGeneratorBuilder(
                                        order -> order.timestamp)
                                .periodicWatermark(Duration.ofMillis(200))
                                .buildAsProcessFunction());

        NonKeyedPartitionStream<ProductSales> productSalesQuantityStream =
                orderStream
                        // key by productId
                        .keyBy(order -> order.productId)
                        .process(
                                BuiltinFuncs.window(
                                        // declare tumbling window with window size 1 hour
                                        WindowStrategy.tumbling(
                                                Duration.ofHours(1), WindowStrategy.EVENT_TIME),
                                        // define window process function to calculate total sales
                                        // quantity per product per window.
                                        new CountSalesQuantity()));

        if (fileOutput) {
            productSalesQuantityStream
                    .toSink(
                            new WrappedSink<>(
                                    FileSink.<ProductSales>forRowFormat(
                                                    new Path(params.get("output")),
                                                    new SimpleStringEncoder<>())
                                            .withRollingPolicy(
                                                    DefaultRollingPolicy.builder()
                                                            .withMaxPartSize(
                                                                    MemorySize.ofMebiBytes(1))
                                                            .withRolloverInterval(
                                                                    Duration.ofSeconds(10))
                                                            .build())
                                            .build()))
                    .withName("file-sink");
        } else {
            // Print the results to the STDOUT.
            productSalesQuantityStream
                    .toSink(new WrappedSink<>(new PrintSink<>()))
                    .withName("print-sink");
        }

        env.execute("Count Product Sales Windowing");
    }

    /**
     * Count sales quantity per product.
     *
     * <p>We will obtain all orders and calculate the total sales quantity for each product when
     * window trigger.
     */
    public static class CountSalesQuantity
            implements OneInputWindowStreamProcessFunction<Order, ProductSales> {

        @Override
        public void onTrigger(
                Collector<ProductSales> output,
                PartitionedContext<ProductSales> ctx,
                OneInputWindowContext<Order> windowContext)
                throws Exception {
            // get current productId
            long productId = ctx.getStateManager().getCurrentKey();
            // calculate total sales quantity
            long salesQuantity = 0;
            for (Order ignored : windowContext.getAllRecords()) {
                salesQuantity += 1;
            }
            // emit result
            output.collect(
                    new ProductSales(productId, windowContext.getStartTime(), salesQuantity));
        }
    }
}
