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

package org.apache.flink.streaming.examples.async;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/** Example to illustrate how to use {@link AsyncFunction}. */
public class AsyncIOExample {

    /** An example of {@link AsyncFunction} using an async client to query an external service. */
    private static class SampleAsyncFunction extends RichAsyncFunction<Integer, String> {
        private static final long serialVersionUID = 1L;

        private transient AsyncClient client;

        @Override
        public void open(OpenContext openContext) {
            client = new AsyncClient();
        }

        @Override
        public void asyncInvoke(final Integer input, final ResultFuture<String> resultFuture) {
            client.query(input)
                    .whenComplete(
                            (response, error) -> {
                                if (response != null) {
                                    resultFuture.complete(Collections.singletonList(response));
                                } else {
                                    resultFuture.completeExceptionally(error);
                                }
                            });
        }
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final String mode;
        final long timeout;

        try {
            mode = params.get("waitMode", "ordered");
            timeout = params.getLong("timeout", 10000L);
        } catch (Exception e) {
            System.out.println(
                    "To customize example, use: AsyncIOExample [--waitMode <ordered or unordered>]");
            throw e;
        }

        // obtain execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataGeneratorSource<Integer> generatorSource =
                new DataGeneratorSource<>(
                        Long::intValue,
                        Integer.MAX_VALUE,
                        RateLimiterStrategy.perSecond(100),
                        Types.INT);

        // create input stream of a single integer
        DataStream<Integer> inputStream =
                env.fromSource(
                        generatorSource,
                        WatermarkStrategy.noWatermarks(),
                        "Integers-generating Source");

        AsyncFunction<Integer, String> function = new SampleAsyncFunction();

        // add async operator to streaming job
        DataStream<String> result;
        switch (mode.toUpperCase()) {
            case "ORDERED":
                result =
                        AsyncDataStream.orderedWait(
                                inputStream, function, timeout, TimeUnit.MILLISECONDS, 20);
                break;
            case "UNORDERED":
                result =
                        AsyncDataStream.unorderedWait(
                                inputStream, function, timeout, TimeUnit.MILLISECONDS, 20);
                break;
            default:
                throw new IllegalStateException("Unknown mode: " + mode);
        }

        result.print();

        // execute the program
        env.execute("Async IO Example: " + mode);
    }
}
