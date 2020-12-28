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

package org.apache.flink.table.runtime.utils;

import org.apache.flink.table.functions.TableFunction;

/** Test functions. */
public class JavaUserDefinedTableFunctions {

    /** Emit inputs as long. */
    public static class JavaTableFunc0 extends TableFunction<Long> {
        public void eval(Integer a, Long b, Long c) {
            collect(a.longValue());
            collect(b);
            collect(c);
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
}
