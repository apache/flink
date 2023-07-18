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

package org.apache.flink.python.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** A test class to test PythonConfigUtil getting executionEnvironment correctly. */
class PythonConfigUtilTest {

    @Test
    void testJobName() {
        String jobName = "MyTestJob";
        Configuration config = new Configuration();
        config.set(PipelineOptions.NAME, jobName);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        env.fromCollection(Collections.singletonList("test")).sinkTo(new DiscardingSink<>());
        StreamGraph streamGraph = env.getStreamGraph(true);
        assertThat(streamGraph.getJobName()).isEqualTo(jobName);
    }
}
