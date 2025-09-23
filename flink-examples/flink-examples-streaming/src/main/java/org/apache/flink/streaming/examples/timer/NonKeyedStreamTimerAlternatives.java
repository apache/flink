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

package org.apache.flink.streaming.examples.timer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Example demonstrating timer limitations with non-keyed streams and alternative approaches.
 *
 * <p>This example shows:
 * <ul>
 *   <li>Why timers cannot be used directly on non-keyed streams
 *   <li>Alternative 1: Using keyed streams with dummy keys for timer functionality
 *   <li>Alternative 2: Using processing time windows for periodic operations
 *   <li>Alternative 3: Using event time windows with watermarks
 * </ul>
 *
 * <p>The example processes a stream of sensor readings and demonstrates different approaches
 * to implement timer-like behavior for non-keyed streams.
 */
public class NonKeyedStreamTimerAlternatives {

    /** A simple sensor reading with timestamp, sensor ID, and temperature. */
    public static class SensorReading {
        public long timestamp;
        public String sensorId;
        public double temperature;

        public SensorReading() {}

        public SensorReading(long timestamp, String sensorId, double temperature) {
            this.timestamp = timestamp;
            this.sensorId = sensorId;
            this.temperature = temperature;
        }

        @Override
        public String toString() {
            return String.format("SensorReading{timestamp=%d, sensorId='%s', temperature=%.1f}", 
                timestamp, sensorId, temperature);
        }
    }

    /** A statistics holder for aggregated sensor data. */
    public static class SensorStats {
        public long windowStart;
        public long windowEnd;
        public int count;
        public double avgTemperature;
        public double minTemperature;
        public double maxTemperature;

        public SensorStats() {}

        public SensorStats(long windowStart, long windowEnd, int count, 
                          double avgTemperature, double minTemperature, double maxTemperature) {
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.count = count;
            this.avgTemperature = avgTemperature;
            this.minTemperature = minTemperature;
            this.maxTemperature = maxTemperature;
        }

        @Override
        public String toString() {
            return String.format("SensorStats{window=[%d-%d], count=%d, avg=%.1f, min=%.1f, max=%.1f}", 
                windowStart, windowEnd, count, avgTemperature, minTemperature, maxTemperature);
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Set parallelism to 1 for clearer output

        // Create a sample data stream of sensor readings
        DataStream<SensorReading> sensorStream = env.fromData(
            new SensorReading(1000L, "sensor_1", 20.1),
            new SensorReading(2000L, "sensor_2", 21.5),
            new SensorReading(3000L, "sensor_1", 19.8),
            new SensorReading(4000L, "sensor_3", 22.3),
            new SensorReading(5000L, "sensor_2", 20.9),
            new SensorReading(6000L, "sensor_1", 18.7),
            new SensorReading(7000L, "sensor_3", 23.1),
            new SensorReading(8000L, "sensor_2", 21.2),
            new SensorReading(9000L, "sensor_1", 19.5),
            new SensorReading(10000L, "sensor_3", 22.8)
        );

        System.out.println("=== Non-Keyed Stream Timer Alternatives Example ===\n");

        // APPROACH 1: Demonstrate the limitation - ProcessFunction on non-keyed stream cannot use timers
        demonstrateTimerLimitation(sensorStream);

        // APPROACH 2: Use keyed stream with dummy key to enable timer functionality
        demonstrateDummyKeyApproach(sensorStream);

        // APPROACH 3: Use processing time windows for periodic operations
        demonstrateProcessingTimeWindowApproach(sensorStream);

        // APPROACH 4: Use event time windows with watermarks
        demonstrateEventTimeWindowApproach(sensorStream);

        env.execute("Non-Keyed Stream Timer Alternatives");
    }

    /**
     * Demonstrates why timers cannot be used on non-keyed streams.
     * This ProcessFunction shows what happens when you try to access TimerService on non-keyed stream.
     */
    private static void demonstrateTimerLimitation(DataStream<SensorReading> sensorStream) {
        System.out.println("1. LIMITATION: ProcessFunction on non-keyed stream cannot use timers");
        
        sensorStream
            .process(new ProcessFunction<SensorReading, String>() {
                @Override
                public void processElement(SensorReading value, Context ctx, Collector<String> out) {
                    // This will demonstrate the limitation
                    try {
                        // Attempting to access timer service on non-keyed stream
                        TimerService timerService = ctx.timerService();
                        
                        // These calls will throw UnsupportedOperationException
                        // timerService.registerProcessingTimeTimer(System.currentTimeMillis() + 5000);
                        
                        out.collect("LIMITATION: Cannot register timers on non-keyed stream. " +
                                   "Current processing time: " + timerService.currentProcessingTime() +
                                   ", Current watermark: " + timerService.currentWatermark());
                        
                    } catch (UnsupportedOperationException e) {
                        out.collect("ERROR: " + e.getMessage());
                    }
                }
            })
            .print("Limitation");

        System.out.println("   Note: Timer registration will fail, but time queries work.\n");
    }

    /**
     * Alternative 1: Use a keyed stream with a dummy key to enable timer functionality.
     * This approach keys all elements with the same key, effectively creating a single partition
     * where timers can be used.
     */
    private static void demonstrateDummyKeyApproach(DataStream<SensorReading> sensorStream) {
        System.out.println("2. ALTERNATIVE 1: Using dummy key to enable timers");
        
        sensorStream
            .keyBy(reading -> "dummy_key") // Key all elements with the same dummy key
            .process(new KeyedProcessFunction<String, SensorReading, String>() {
                private ValueState<Integer> countState;
                private ValueState<Long> lastTimerState;

                @Override
                public void open(OpenContext openContext) throws Exception {
                    countState = getRuntimeContext().getState(
                        new ValueStateDescriptor<>("count", Types.INT));
                    lastTimerState = getRuntimeContext().getState(
                        new ValueStateDescriptor<>("lastTimer", Types.LONG));
                }

                @Override
                public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
                    // Update count
                    Integer currentCount = countState.value();
                    if (currentCount == null) {
                        currentCount = 0;
                    }
                    countState.update(currentCount + 1);

                    // Register a timer 3 seconds from now (only if no timer is pending)
                    Long lastTimer = lastTimerState.value();
                    long currentTime = ctx.timerService().currentProcessingTime();
                    
                    if (lastTimer == null || currentTime >= lastTimer) {
                        long timerTime = currentTime + 3000; // 3 seconds from now
                        ctx.timerService().registerProcessingTimeTimer(timerTime);
                        lastTimerState.update(timerTime);
                        
                        out.collect(String.format("Processed %s (count: %d), Timer set for %d", 
                            value, currentCount + 1, timerTime));
                    } else {
                        out.collect(String.format("Processed %s (count: %d), Timer already pending", 
                            value, currentCount + 1));
                    }
                }

                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                    Integer count = countState.value();
                    out.collect(String.format("TIMER FIRED at %d: Processed %d readings so far", 
                        timestamp, count != null ? count : 0));
                    
                    // Clear the timer state so new timers can be registered
                    lastTimerState.clear();
                }
            })
            .print("DummyKey");

        System.out.println("   This approach works but processes all data in a single partition.\n");
    }

    /**
     * Alternative 2: Use processing time windows for periodic operations.
     * This approach uses tumbling processing time windows to trigger periodic computations.
     */
    private static void demonstrateProcessingTimeWindowApproach(DataStream<SensorReading> sensorStream) {
        System.out.println("3. ALTERNATIVE 2: Using processing time windows");
        
        sensorStream
            .keyBy(reading -> "all") // Use a single key to aggregate all data
            .window(TumblingProcessingTimeWindows.of(Time.seconds(4)))
            .process(new ProcessWindowFunction<SensorReading, SensorStats, String, TimeWindow>() {
                @Override
                public void process(String key, Context context, 
                                  Iterable<SensorReading> elements, Collector<SensorStats> out) {
                    
                    int count = 0;
                    double sum = 0.0;
                    double min = Double.MAX_VALUE;
                    double max = Double.MIN_VALUE;
                    
                    for (SensorReading reading : elements) {
                        count++;
                        sum += reading.temperature;
                        min = Math.min(min, reading.temperature);
                        max = Math.max(max, reading.temperature);
                    }
                    
                    if (count > 0) {
                        double avg = sum / count;
                        SensorStats stats = new SensorStats(
                            context.window().getStart(),
                            context.window().getEnd(),
                            count, avg, min, max
                        );
                        out.collect(stats);
                    }
                }
            })
            .print("ProcessingTimeWindow");

        System.out.println("   This approach provides periodic processing based on wall-clock time.\n");
    }

    /**
     * Alternative 3: Use event time windows with watermarks for time-based processing.
     * This approach uses event time from the data and watermarks to trigger computations.
     */
    private static void demonstrateEventTimeWindowApproach(DataStream<SensorReading> sensorStream) {
        System.out.println("4. ALTERNATIVE 3: Using event time windows with watermarks");
        
        sensorStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((reading, timestamp) -> reading.timestamp))
            .keyBy(reading -> "all") // Use a single key to aggregate all data
            .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
            .process(new ProcessWindowFunction<SensorReading, String, String, TimeWindow>() {
                @Override
                public void process(String key, Context context, 
                                  Iterable<SensorReading> elements, Collector<String> out) {
                    
                    int count = 0;
                    StringBuilder sensors = new StringBuilder();
                    
                    for (SensorReading reading : elements) {
                        count++;
                        if (sensors.length() > 0) {
                            sensors.append(", ");
                        }
                        sensors.append(reading.sensorId);
                    }
                    
                    out.collect(String.format("EventTime Window [%d-%d]: %d readings from sensors: %s", 
                        context.window().getStart(), context.window().getEnd(), count, sensors.toString()));
                }
            })
            .print("EventTimeWindow");

        System.out.println("   This approach uses event time from data for deterministic processing.\n");
    }
}
