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

package org.apache.flink.quickstarts.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.quickstarts.test.utils.Utils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

/** End to end test for quickstarts. */
public class QuickstartExample {

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 1) {
            System.out.println("Missing parameters!\nUsage: --numRecords <numRecords>");
            return;
        }

        int numRecordsToEmit = parameterTool.getInt("numRecords");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        DataStream<String> source =
                env.fromSequence(0, numRecordsToEmit - 1)
                        .map((MapFunction<Long, String>) Utils::prefix);

        try (CloseableIterator<String> data = source.collectAsync()) {
            env.execute("Quickstart example");

            int count = 0;
            while (data.hasNext()) {
                data.next();
                count++;
            }
            if (count != numRecordsToEmit) {
                throw new RuntimeException(
                        String.format(
                                "Unexpected number of records; expected :%s actual: %s",
                                numRecordsToEmit, count));
            }
        }
    }
}
