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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Simple example demonstrating that timers cannot be used on non-keyed streams.
 *
 * <p>This example shows the fundamental limitation: {@link ProcessFunction} on non-keyed streams
 * cannot register or delete timers. The {@link org.apache.flink.streaming.api.TimerService}
 * throws {@link UnsupportedOperationException} when trying to register timers on non-keyed streams.
 *
 * <p>Key points demonstrated:
 * <ul>
 *   <li>Non-keyed streams can access current time information (processing time, watermark)
 *   <li>Non-keyed streams cannot register processing time or event time timers
 *   <li>Non-keyed streams cannot delete timers
 *   <li>The limitation is enforced at runtime with clear error messages
 * </ul>
 *
 * <p>For timer functionality on non-keyed streams, see {@link NonKeyedStreamTimerAlternatives}
 * which demonstrates several workaround approaches.
 */
public class NonKeyedStreamTimerLimitation {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a simple non-keyed data stream
        DataStream<String> dataStream = env.fromData(
            "first", "second", "third", "fourth", "fifth"
        );

        System.out.println("=== Non-Keyed Stream Timer Limitation Example ===");
        System.out.println("This example demonstrates why timers cannot be used on non-keyed streams.\n");

        // Apply ProcessFunction to non-keyed stream - this will show the timer limitations
        dataStream
            .process(new ProcessFunction<String, String>() {
                @Override
                public void processElement(String value, Context ctx, Collector<String> out) {
                    // These operations work fine on non-keyed streams:
                    long currentProcessingTime = ctx.timerService().currentProcessingTime();
                    long currentWatermark = ctx.timerService().currentWatermark();
                    
                    out.collect(String.format("Processing '%s' - ProcessingTime: %d, Watermark: %d", 
                        value, currentProcessingTime, currentWatermark));

                    // These operations will throw UnsupportedOperationException:
                    try {
                        // Attempt to register a processing time timer
                        ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000);
                        out.collect("ERROR: Timer registration should have failed!");
                    } catch (UnsupportedOperationException e) {
                        out.collect("EXPECTED: " + e.getMessage());
                    }

                    try {
                        // Attempt to register an event time timer  
                        ctx.timerService().registerEventTimeTimer(currentProcessingTime + 5000);
                        out.collect("ERROR: Event time timer registration should have failed!");
                    } catch (UnsupportedOperationException e) {
                        out.collect("EXPECTED: " + e.getMessage());
                    }

                    try {
                        // Attempt to delete a processing time timer
                        ctx.timerService().deleteProcessingTimeTimer(currentProcessingTime + 5000);
                        out.collect("ERROR: Timer deletion should have failed!");
                    } catch (UnsupportedOperationException e) {
                        out.collect("EXPECTED: " + e.getMessage());
                    }

                    out.collect("---");
                }
            })
            .print();

        System.out.println("\nRunning the job...");
        System.out.println("Expected output: Time queries work, but timer operations fail with clear error messages.");
        System.out.println("For alternatives, see NonKeyedStreamTimerAlternatives.java\n");

        env.execute("Non-Keyed Stream Timer Limitation");
    }
}
