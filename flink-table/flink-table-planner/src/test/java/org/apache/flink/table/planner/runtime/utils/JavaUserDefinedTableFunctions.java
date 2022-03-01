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
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.TableFunction;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Random;

/** Test functions. */
public class JavaUserDefinedTableFunctions {

    /** Emit inputs as long. */
    public static class JavaTableFunc0 extends TableFunction<Long> {
        public void eval(Integer a, Long b, TimestampData c) {
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
