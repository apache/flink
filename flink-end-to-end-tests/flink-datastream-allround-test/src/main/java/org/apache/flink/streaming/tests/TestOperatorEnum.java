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

package org.apache.flink.streaming.tests;

/** Enum of names and uids of all test operators used in {@link DataStreamAllroundTestProgram}. */
public enum TestOperatorEnum {
    EVENT_SOURCE("EventSource", 1),
    KEYED_STATE_OPER_WITH_KRYO_AND_CUSTOM_SER(
            "ArtificalKeyedStateMapper_Kryo_and_Custom_Stateful", 2),
    KEYED_STATE_OPER_WITH_AVRO_SER("ArtificalKeyedStateMapper_Avro", 3),
    OPERATOR_STATE_OPER("ArtificalOperatorStateMapper", 4),
    TIME_WINDOW_OPER("TumblingWindowOperator", 5),
    FAILURE_MAPPER_NAME("FailureMapper", 6),
    SEMANTICS_CHECK_MAPPER("SemanticsCheckMapper", 7),
    SEMANTICS_CHECK_PRINT_SINK("SemanticsCheckPrintSink", 8),
    SLIDING_WINDOW_AGG("SlidingWindowOperator", 9),
    SLIDING_WINDOW_CHECK_MAPPER("SlidingWindowCheckMapper", 10),
    SLIDING_WINDOW_CHECK_PRINT_SINK("SlidingWindowCheckPrintSink", 11),
    RESULT_TYPE_QUERYABLE_MAPPER_WITH_CUSTOM_SER(
            "ResultTypeQueryableMapWithCustomStatefulTypeSerializer", 12),
    MAPPER_RETURNS_OUT_WITH_CUSTOM_SER("MapReturnsOutputWithCustomStatefulTypeSerializer", 13),
    EVENT_IDENTITY_MAPPER("EventIdentityMapper", 14);

    private final String name;
    private final String uid;

    TestOperatorEnum(String name, int uid) {
        this.name = name;
        this.uid = String.format("%04d", uid);
    }

    public String getName() {
        return name;
    }

    public String getUid() {
        return uid;
    }
}
