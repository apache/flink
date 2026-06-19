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

package org.apache.flink.table.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;

import org.junit.Assert;

import java.util.Arrays;
import java.util.Iterator;

/** Testing Function for python udf tests. */
public class TestingFunctions {

    /** RichFunc0 for testing. */
    public static class RichFunc0 extends ScalarFunction {
        private boolean openCalled = false;
        private boolean closeCalled = false;

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            if (openCalled) {
                Assert.fail("Open called more than once.");
            } else {
                openCalled = true;
            }

            if (closeCalled) {
                Assert.fail("Close called before open.");
            }
        }

        public Integer eval(Integer index) {
            if (!openCalled) {
                Assert.fail("Open was not called before eval.");
            }

            if (closeCalled) {
                Assert.fail("Close called before eval.");
            }

            return index + 1;
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (closeCalled) {
                Assert.fail("Close called more than once.");
            } else {
                closeCalled = true;
            }

            if (!openCalled) {
                Assert.fail("Open was not called before close.");
            }
        }
    }

    /** MaxAccumulator for testing. */
    static class MaxAccumulator<T extends Comparable> extends Tuple2<T, Boolean> {}

    /** MaxAggFunction for testing. */
    abstract static class MaxAggFunction<T extends Comparable>
            extends AggregateFunction<T, MaxAccumulator<T>> {
        @Override
        public T getValue(MaxAccumulator<T> accumulator) {
            if (accumulator.f1) {
                return accumulator.f0;
            } else {
                return null;
            }
        }

        @Override
        public MaxAccumulator<T> createAccumulator() {
            MaxAccumulator<T> accumulator = new MaxAccumulator<>();
            accumulator.f0 = getInitValue();
            accumulator.f1 = false;
            return accumulator;
        }

        public void accumulate(MaxAccumulator<T> acc, Object value) {
            if (value != null) {
                T t = (T) value;
                if (!acc.f1 || acc.f0.compareTo(t) < 0) {
                    acc.f0 = t;
                    acc.f1 = true;
                }
            }
        }

        public void merge(MaxAccumulator<T> accumulator, Iterable<MaxAccumulator<T>> its) {
            Iterator<MaxAccumulator<T>> iter = its.iterator();
            while (iter.hasNext()) {
                MaxAccumulator<T> a = iter.next();
                if (a.f1) {
                    accumulate(accumulator, a.f0);
                }
            }
        }

        public void resetAccumulator(MaxAccumulator<T> accumulator) {
            accumulator.f0 = getInitValue();
            accumulator.f1 = false;
        }

        abstract T getInitValue();

        abstract TypeInformation<?> getValueTypeInfo();
    }

    /** ByteMaxAggFunction for testing. */
    public static class ByteMaxAggFunction extends MaxAggFunction<Byte> {

        @Override
        Byte getInitValue() {
            return 0;
        }

        @Override
        TypeInformation<?> getValueTypeInfo() {
            return Types.BYTE;
        }
    }

    /** TableFunc1 for testing. */
    public static class TableFunc1 extends TableFunction<String> {
        public void eval(String str) {
            if (str.contains("#")) {
                Arrays.stream(str.split("#")).forEach(this::collect);
            }
        }

        public void eval(String str, String prefix) {
            if (str.contains("#")) {
                Arrays.stream(str.split("#")).forEach(s -> collect(prefix + s));
            }
        }
    }
}
