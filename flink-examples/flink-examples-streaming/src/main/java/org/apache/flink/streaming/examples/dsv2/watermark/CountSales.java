/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.dsv2.watermark;

import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclarations;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.api.connector.dsv2.WrappedSource;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.ApplyPartitionFunction;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.examples.dsv2.eventtime.CountNewsClicks;
import org.apache.flink.util.ParameterTool;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This example shows how to count the cumulative sales of each product at the moment. In this
 * example, we simulate the creation and propagation of event time watermarks through {@link
 * Watermark}.
 *
 * <p>Please note that Flink provides users with an event time extension to support event time. You
 * can refer to the {@link CountNewsClicks} example.
 *
 * <p>The example uses a {@link DataGeneratorSource} as input source and a custom {@link Watermark}
 * to propagate the event time in the stream.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li><code>--parallelism &lt;path&gt;</code>The parallelism of the source. The default value is
 *       5.
 * </ul>
 *
 * <p>This example shows how to:
 *
 * <ul>
 *   <li>How to define and propagate custom event (Watermark) in DataStream API V2
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
public class CountSales {

    /** Pojo class for Order. */
    public static class Order {
        public long productId;
        public double totalPrice;
        public long timestamp;

        public Order(long productId, double price, long timestamp) {
            this.productId = productId;
            this.totalPrice = price;
            this.timestamp = timestamp;
        }
    }

    /** {@link CumulativeSales} represents the cumulative sales at a certain moment of a product. */
    public static class CumulativeSales {
        public long productId;
        public long timestamp;
        public double sales;

        public CumulativeSales(long productId, long timestamp, double sales) {
            this.productId = productId;
            this.timestamp = timestamp;
            this.sales = sales;
        }

        @Override
        public String toString() {
            return String.format("%d,%d,%.2f", this.productId, this.timestamp, this.sales);
        }
    }

    /**
     * Firstly, we define an event time watermark, which represents the time of currently processing
     * event. Since the watermark needs to convey the timestamp, its data type is long. To determine
     * the minimum event time across all watermarks, we utilize the combineFunctionMin() method to
     * combine the watermarks. The default handling strategy is forward, meaning that the watermark
     * will typically be advanced to downstream operators in most scenarios. Thus, we create a
     * WatermarkDeclaration instance that can be used to declare and generate the watermark.
     */
    public static final LongWatermarkDeclaration EVENT_TIME_WATERMARK_DECLARATION =
            WatermarkDeclarations.newBuilder("EVENT_TIME")
                    .typeLong()
                    .combineFunctionMin()
                    .combineWaitForAllChannels(true)
                    .defaultHandlingStrategyForward()
                    .build();

    public static void main(String[] args) throws Exception {
        // parse the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final int parallelism = params.getInt("parallelism", 5);

        // obtain execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        // Create the Order source, the source will declare and generate event time watermarks.
        NonKeyedPartitionStream<Order> source =
                env.fromSource(new WrappedSource<>(new OrderSource()), "order source")
                        .withParallelism(parallelism);

        source
                // key by product id
                .keyBy(order -> order.productId)
                .process(
                        // handle event time watermark in downstream
                        new CountSalesProcessFunction())
                .toSink(new WrappedSink<>(new PrintSink<>()));

        // execute program
        env.execute("Count Sales");
    }

    /** Source of Orders. We will declare and generate the event time watermark in this source. */
    private static class OrderSource extends DataGeneratorSource<Order> {

        public OrderSource() {
            super(new CustomGeneratorFunction(), 1_000_000L, TypeInformation.of(Order.class));
        }

        @Override
        public Set<? extends WatermarkDeclaration> declareWatermarks() {
            // Declare the event time watermark.
            return Set.of(EVENT_TIME_WATERMARK_DECLARATION);
        }
    }

    /**
     * Generator function for the Order source. We will generate event time watermark every second
     * within this generator function.
     */
    private static class CustomGeneratorFunction implements GeneratorFunction<Long, Order> {

        private SourceReaderContext readerContext;

        private long previousTimestamp = 0;

        @Override
        public void open(SourceReaderContext readerContext) throws Exception {
            this.readerContext = readerContext;
        }

        @Override
        public Order map(Long value) throws Exception {
            long productId = ThreadLocalRandom.current().nextLong(1, 100);
            double price = ThreadLocalRandom.current().nextDouble(1, 1000);
            long timestamp = System.currentTimeMillis();
            if (timestamp - previousTimestamp > Duration.ofSeconds(1).toMillis()) {
                // Send event time watermark
                readerContext.emitWatermark(
                        EVENT_TIME_WATERMARK_DECLARATION.newWatermark(timestamp));
                previousTimestamp = timestamp;
            }

            Thread.sleep(100);
            return new Order(productId, price, timestamp);
        }
    }

    /**
     * This process function calculates the cumulative sales for each product. It utilizes a
     * ValueState to store the cumulative sales and updates this state whenever a new record is
     * received. Additionally, it emits the cumulative sales for each product when an event-time
     * watermark arrives.
     */
    private static class CountSalesProcessFunction
            implements OneInputStreamProcessFunction<Order, CumulativeSales> {

        // uses a ValueState to store the sales of each product
        private final ValueStateDeclaration<Double> salesStateDeclaration =
                StateDeclarations.valueState("sales", TypeDescriptors.DOUBLE);

        @Override
        public Set<StateDeclaration> usesStates() {
            return Set.of(salesStateDeclaration);
        }

        @Override
        public void processRecord(
                Order record,
                Collector<CumulativeSales> output,
                PartitionedContext<CumulativeSales> ctx)
                throws Exception {
            // update the sales state
            ValueState<Double> salesState = ctx.getStateManager().getState(salesStateDeclaration);
            Double previousSales = salesState.value();
            Double newlySales =
                    previousSales == null ? record.totalPrice : previousSales + record.totalPrice;
            salesState.update(newlySales);
        }

        @Override
        public WatermarkHandlingResult onWatermark(
                Watermark watermark,
                Collector<CumulativeSales> output,
                NonPartitionedContext<CumulativeSales> ctx) {
            if (watermark
                    .getIdentifier()
                    .equals(EVENT_TIME_WATERMARK_DECLARATION.getIdentifier())) {
                // For our custom event time watermark, we can get current event time and output
                // sales result
                long currentEventTime = ((LongWatermark) watermark).getValue();
                try {
                    ctx.applyToAllPartitions(
                            new ApplyPartitionFunction<CumulativeSales>() {

                                @Override
                                public void apply(
                                        Collector<CumulativeSales> collector,
                                        PartitionedContext<CumulativeSales> ctx)
                                        throws Exception {
                                    long productId = ctx.getStateManager().getCurrentKey();
                                    Double sales =
                                            ctx.getStateManager()
                                                    .getState(salesStateDeclaration)
                                                    .value();
                                    sales = sales == null ? 0 : sales;
                                    collector.collect(
                                            new CumulativeSales(
                                                    productId, currentEventTime, sales));
                                }
                            });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                return WatermarkHandlingResult.PEEK;
            }

            return WatermarkHandlingResult.PEEK;
        }
    }
}
