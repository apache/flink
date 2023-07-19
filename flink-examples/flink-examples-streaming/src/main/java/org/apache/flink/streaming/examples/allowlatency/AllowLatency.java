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

package org.apache.flink.streaming.examples.allowlatency;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.allowlatency.utils.CountingAndDiscardingSink;
import org.apache.flink.streaming.examples.allowlatency.utils.MyAggregationSource;
import org.apache.flink.streaming.examples.allowlatency.utils.MyAggregator;

import java.util.Map;


/**
 * Example illustrating aggregations on data stream supporting user define end-to-end allowed
 * latency.
 *
 * <p>The example pipeline aggregates the data with configurable checkpoint interval and flush
 * latency.
 *
 * <p>The example uses a sample data generator that generates the data with user defined key size.
 *
 * <p>Usage:
 *
 * <p><code>
 *     AllowLatency --execution.checkpointing.interval &lt;n&gt --execution.allowed-latency &lt;n&gt;
 *     --dataNum &lt;n&gt; --keySize &lt;n&gt;
 * </code>
 */
public class AllowLatency {
    public static void main(String[] args) throws Exception {
        final Map<String, String> params = MultipleParameterTool.fromArgs(args).toMap();
        Configuration config = new Configuration();
        config.setBoolean(DeploymentOptions.ATTACHED, true);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.getConfig().disableGenericTypes();
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(0);
        env.getCheckpointConfig().disableCheckpointing();
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
//                env.setStateBackend(new HashMapStateBackend());

        Configuration conf = Configuration.fromMap(params);
        env.getCheckpointConfig().configure(conf);
        env.getConfig().configure(conf, Thread.currentThread().getContextClassLoader());

        long dataNum =
                params.get("dataNum") == null ? 50000000 : Long.parseLong(params.get("dataNum"));
        int keySize =
                params.get("keySize") == null ? 30000 : Integer.parseInt(params.get("keySize"));
        System.out.println("Number of records: " + dataNum);
        System.out.println(
                "Checkpoint interval: " + env.getCheckpointConfig().getCheckpointInterval());
        System.out.println("Allowed latency: " + env.getConfig().getAllowedLatency());

        DataStream<Integer> ds1 = env.addSource(new MyAggregationSource(dataNum, 0, keySize));
        ds1.keyBy(value -> value)
                .transform(
                        "MyAggregator",
                        new TupleTypeInfo<>(
                                IntegerTypeInfo.INT_TYPE_INFO, IntegerTypeInfo.LONG_TYPE_INFO),
                        new MyAggregator(value -> value))
                .addSink(new CountingAndDiscardingSink<>());
        //                .addSink(
        //                        new SinkFunction<Tuple2<Integer, Long>>() {
        //                            @Override
        //                            public void invoke(Tuple2<Integer, Long> value) throws
        // Exception {
        //                                Long v = testMap.get(value.f0);
        //                                if (v == null || v < value.f1) {
        //                                    testMap.put(value.f0, value.f1);
        //                                }
        //                            }
        //                        });

        long startTime = System.currentTimeMillis();
        JobExecutionResult executionResult = env.execute("");
        long endTime = System.currentTimeMillis();
        System.out.println("Duration: " + (endTime - startTime));
        System.out.println("Throughput: " + (dataNum / (endTime - startTime)));
        //        testMap.forEach(
        //                (k, v) -> {
        //                    System.out.println(k + ": " + v);
        //                });
    }
}
