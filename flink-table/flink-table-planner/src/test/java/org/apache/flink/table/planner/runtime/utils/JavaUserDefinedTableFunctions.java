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

package org.apache.flink.table.planner.runtime.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/** Test functions. */
public class JavaUserDefinedTableFunctions {

    /** Emit inputs as long. */
    public static class JavaTableFunc0 extends TableFunction<Long> {
        public void eval(
                @DataTypeHint("DATE") Integer a,
                Long b,
                @DataTypeHint("TIMESTAMP(0)") TimestampData c) {
            collect(a.longValue());
            collect(b);
            collect(c.getMillisecond());
        }
    }

    /** Emit every input string. */
    public static class JavaVarsArgTableFunc0 extends TableFunction<String> {
        public void eval(String... strs) {
            for (String s : strs) {
                collect(s);
            }
        }

        public void eval(int val, String str) {
            for (int i = 0; i < val; i++) {
                collect(str);
            }
        }
    }

    /** Emit sum of String length of all parameters. */
    public static class JavaTableFunc1 extends TableFunction<Integer> {
        public void eval(String... strs) {
            int sum = 0;
            if (strs != null) {
                for (String str : strs) {
                    sum += str == null ? 0 : str.length();
                }
            }
            collect(sum);
        }
    }

    /** String split table function. */
    public static class StringSplit extends TableFunction<String> {

        public void eval() {
            String[] strs = {"a", "b", "c"};
            for (String str : strs) {
                eval(str);
            }
        }

        public void eval(String str) {
            this.eval(str, ",");
        }

        public void eval(String str, String separatorChars) {
            this.eval(str, separatorChars, 0);
        }

        public void eval(String str, String separatorChars, int startIndex) {
            if (str != null) {
                String[] strs = StringUtils.split(str, separatorChars);
                if (startIndex < 0) {
                    startIndex = 0;
                }
                for (int i = startIndex; i < strs.length; ++i) {
                    collect(strs[i]);
                }
            }
        }

        public void eval(byte[] varbinary) {
            if (varbinary != null) {
                this.eval(new String(varbinary, StandardCharsets.UTF_8));
            }
        }

        @Override
        public TypeInformation<String> getResultType() {
            return Types.STRING;
        }
    }

    /** String split table function. */
    public static class AsyncStringSplit extends AsyncTableFunction<String> {

        public void eval(CompletableFuture<Collection<String>> future) {
            String[] strs = {"a", "b", "c"};
            for (String str : strs) {
                eval(future, str);
            }
        }

        public void eval(CompletableFuture<Collection<String>> future, String str) {
            this.eval(future, str, ",");
        }

        public void eval(
                CompletableFuture<Collection<String>> future, String str, String separatorChars) {
            this.eval(future, str, separatorChars, 0);
        }

        public void eval(
                CompletableFuture<Collection<String>> future,
                String str,
                String separatorChars,
                int startIndex) {
            if (str != null) {
                String[] strs = StringUtils.split(str, separatorChars);
                if (startIndex < 0) {
                    startIndex = 0;
                }
                List<String> result =
                        new ArrayList<>(Arrays.asList(strs).subList(startIndex, strs.length));
                future.complete(result);
            }
        }

        public void eval(CompletableFuture<Collection<String>> future, byte[] varbinary) {
            if (varbinary != null) {
                this.eval(future, new String(varbinary, StandardCharsets.UTF_8));
            }
        }
    }

    /** A table function. */
    @FunctionHint(output = @DataTypeHint("ROW<s STRING >"))
    public static class AsyncTestTableFunction extends AsyncTableFunction<Row> {

        public void eval(CompletableFuture<Collection<Row>> result, Integer i) {
            result.complete(Arrays.asList(Row.of("blah " + i), Row.of("foo " + i)));
        }
    }

    /** A sum function. */
    public static class SumScalarFunction extends ScalarFunction {

        public int eval(int a, int b) {
            return a + b;
        }
    }

    /** A sum function. */
    public static class AsyncSumScalarFunction extends AsyncScalarFunction {

        public void eval(CompletableFuture<Integer> result, int a, int b) {
            result.complete(a + b);
        }
    }

    /** Non-deterministic table function. */
    public static class NonDeterministicTableFunc extends TableFunction<String> {

        Random random = new Random();

        public void eval(String str) {
            String[] values = str.split("#");
            collect(values[random.nextInt(values.length)]);
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }
    }

    /** Function with large tuple. */
    public static class JavaTableFuncTuple12
            extends TableFunction<
                    Tuple12<
                            String,
                            String,
                            String,
                            String,
                            String,
                            String,
                            Integer,
                            Integer,
                            Integer,
                            Integer,
                            Integer,
                            Integer>> {
        private static final long serialVersionUID = -8258882510989374448L;

        public void eval(String str) {
            collect(
                    Tuple12.of(
                            str + "_a",
                            str + "_b",
                            str + "_c",
                            str + "_d",
                            str + "_e",
                            str + "_f",
                            str.length(),
                            str.length() + 1,
                            str.length() + 2,
                            str.length() + 3,
                            str.length() + 4,
                            str.length() + 5));
        }
    }
}
