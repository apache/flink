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

package org.apache.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AsyncSinkHangDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // Checkpoint every 5 seconds - will timeout due to hang

        // Generate continuous stream of data using DataGeneratorSource
        DataGeneratorSource<String> source =
                new DataGeneratorSource<>(
                        (Long index) -> "Record-" + index,
                        1000, // records per second
                        Types.STRING);

        DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "DataGenerator");

        // AsyncSink with rate limiting - demonstrates the hang when rate limiting blocks with no
        // in-flight requests
        // Configuration: maxInFlightRequests=1, tokensPerSecond=5, tokensPerMinute=100
        // This will cause tokens to be exhausted quickly, triggering the hang scenario:
        // 1. Tokens get consumed by initial requests
        // 2. shouldBlock() returns true (no tokens available)
        // 3. currentInFlightRequests drops to 0 (requests complete fast)
        // 4. flush() loops on mailboxExecutor.yield() indefinitely
        // 5. Checkpoint cannot complete -> timeout -> job failure
        TokenBucketRateLimitingStrategy rateLimiter =
                new TokenBucketRateLimitingStrategy(1, 5, 100);
        DummyAsyncSink sink = new DummyAsyncSink(rateLimiter);
        stream.sinkTo(sink);

        // File sink - original implementation (commented out)
        /*
        FileSink<String> sink = FileSink
                .forRowFormat(new Path("/tmp/flink-output"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                    DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.ofMinutes(1))
                        .withInactivityInterval(Duration.ofSeconds(30))
                        .withMaxPartSize(MemorySize.ofMebiBytes(1))
                        .build())
                .build();

        stream.sinkTo(sink);
        */

        env.execute("AsyncSink Hang Demo");
    }
}
