package org.apache.flink.table.planner.runtime.stream.table;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PTFITCase {

    private TableEnvironment tEnv;
    private StreamExecutionEnvironment env;

    @BeforeEach
    public void before() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());
    }

    @Test
    public void testLongScalarNoCast() {
        tEnv.createTemporarySystemFunction("func", new GreetingLongScalar());
        tEnv.executeSql("CREATE VIEW Names(name) AS VALUES ('Bob'), ('Alice'), ('Bob')");

        final List<Row> results =
                executeSql("SELECT * FROM func(TABLE Names PARTITION BY name, 100)");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, "Bob", "Hello 100!"),
                        Row.ofKind(RowKind.INSERT, "Alice", "Hello 100!"),
                        Row.ofKind(RowKind.INSERT, "Bob", "Hello 100!"));

        assertThat(results).containsSequence(expectedRows);
    }

    private List<Row> executeSql(String sql) {
        TableResult result = tEnv.executeSql(sql);
        final List<Row> rows = new ArrayList<>();
        result.collect().forEachRemaining(rows::add);
        return rows;
    }

    /** A PTF that takes an int argument and returns a greeting. */
    public static class GreetingLongScalar extends ProcessTableFunction<String> {
        public void eval(
                @ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input,
                long arg) {
            collect("Hello " + arg + "!");
        }
    }
}
