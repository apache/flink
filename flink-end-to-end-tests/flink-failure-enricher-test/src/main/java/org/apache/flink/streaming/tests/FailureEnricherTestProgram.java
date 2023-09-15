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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * End-to-end test program for verifying that the {@link
 * org.apache.flink.configuration.JobManagerOptions#FAILURE_ENRICHERS_LIST}. We test this by
 * creating a {@code CustomTestFailureEnricherFactory} and {@code CustomTestFailureEnricher} which
 * will add label for the failure. And we will add this jar to plugin/failure-enricher package, then
 * submit this job and verify the exceptions through restful api in test_failure_enricher.sh script.
 */
public class FailureEnricherTestProgram {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements("Hello")
                .map(
                        (MapFunction<String, String>)
                                value -> {
                                    throw new RuntimeException("Expect exception");
                                })
                .writeAsText(params.getRequired("output"), FileSystem.WriteMode.OVERWRITE);

        env.execute("Failure Enricher Test");
    }
}
