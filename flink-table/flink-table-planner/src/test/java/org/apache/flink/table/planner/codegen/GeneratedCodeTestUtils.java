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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/** Utilities for tests that check the code generated for a query. */
final class GeneratedCodeTestUtils {

    private GeneratedCodeTestUtils() {}

    static List<Row> collect(TableEnvironment tEnv, String sql) {
        return CollectionUtil.iteratorToList(tEnv.executeSql(sql).collect());
    }

    /** Returns the code of the classes generated for the operators of the query. */
    static List<String> generatedClassCodes(StreamTableEnvironment tEnv, String sql) {
        final Transformation<?> root =
                tEnv.toChangelogStream(tEnv.sqlQuery(sql)).getTransformation();
        final List<String> codes = new ArrayList<>();
        for (Transformation<?> transformation : root.getTransitivePredecessors()) {
            if (transformation instanceof OneInputTransformation) {
                final StreamOperatorFactory<?> factory =
                        ((OneInputTransformation<?, ?>) transformation).getOperatorFactory();
                if (factory instanceof CodeGenOperatorFactory) {
                    codes.add(((CodeGenOperatorFactory<?>) factory).getGeneratedClass().getCode());
                }
            }
        }
        return codes;
    }

    static int countMatches(Pattern pattern, String code) {
        return (int) pattern.matcher(code).results().count();
    }
}
