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

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class GeneratorSourceCheck {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(2);

        //        DataStreamSource<String> ds = env.fromGenerator(generator, 10, Types.STRING);
        //        ds.print();

        /*        MapFunction<Long, String> generator = value -> ">>> " + value;
        GeneratorSource<String> from = GeneratorSource.from(generator, 10, Types.STRING);
        DataStreamSource<String> watermarked =
                env.fromSource(from, WatermarkStrategy.noWatermarks(), "watermarked");
                watermarked.print();*/

        DataStreamSource<Long> longDataStreamSource = env.fromSequence(0, 10);
        longDataStreamSource.print();

        // ---
        //        MapFunction<Long, String> generator2 = value -> ">>>>>> " + value;
        //        SingleOutputStreamOperator<String> ds2 = env.fromFunction(generator2, 10);
        //        ds2.print();

        // Apache Flink applications are composed lazily. Calling execute
        // submits the Job and begins processing.
        env.execute("WordCount");
    }
}
