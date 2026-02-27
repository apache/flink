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

package org.apache.flink.table.test.join;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;

import static org.apache.flink.table.api.Expressions.$;

/** Example application that tests JoinWithCustomType with Table API. */
public class JoinWithCustomTypeExample {

    private static final TypeInformation<Integer> INT = Types.INT;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        runTest(env, tEnv);
    }

    private static void runTest(StreamExecutionEnvironment env, StreamTableEnvironment tEnv)
            throws Exception {
        System.out.println("Running join with custom type test...");

        TestClass value = new TestClass();
        TypeInformation<TestClass> valueTypeInfo =
                new PojoTypeInfo<>(TestClass.class, new ArrayList<>());

        Table table1 =
                tEnv.fromDataStream(
                        env.fromData(Row.of(1)).returns(Types.ROW_NAMED(new String[] {"id"}, INT)));

        Table table2 =
                tEnv.fromDataStream(
                        env.fromData(Row.of(1, value))
                                .returns(
                                        Types.ROW_NAMED(
                                                new String[] {"id2", "value"},
                                                INT,
                                                valueTypeInfo)));

        tEnv.toDataStream(table1.leftOuterJoin(table2, $("id").isEqual($("id2"))))
                .sinkTo(new DiscardingSink<>());

        env.execute("joinWithCustomType");
        System.out.println("Job joinWithCustomType completed successfully!");
    }
}
