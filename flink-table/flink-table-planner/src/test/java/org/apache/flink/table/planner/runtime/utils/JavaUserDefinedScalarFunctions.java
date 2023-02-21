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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.fail;

/** Test scalar functions. */
public class JavaUserDefinedScalarFunctions {

    /** Increment input. */
    public static class JavaFunc0 extends ScalarFunction {
        public long eval(Long l) {
            return l + 1;
        }
    }

    /** Concatenate inputs as strings. */
    public static class JavaFunc1 extends ScalarFunction {
        public String eval(Integer a, int b, @DataTypeHint("TIMESTAMP(3)") TimestampData c) {
            Long ts = (c == null) ? null : c.getMillisecond();
            return a + " and " + b + " and " + ts;
        }
    }

    /** Append product to string. */
    public static class JavaFunc2 extends ScalarFunction {
        public String eval(String s, Integer... a) {
            int m = 1;
            for (int n : a) {
                m *= n;
            }
            return s + m;
        }
    }

    /**
     * A UDF minus Timestamp with the specified offset. This UDF also ensures open and close are
     * called.
     */
    public static class JavaFunc5 extends ScalarFunction {
        // these fields must be reset to false at the beginning of tests,
        // otherwise the static fields will be changed by several tests concurrently
        public static boolean openCalled = false;
        public static boolean closeCalled = false;

        @Override
        public void open(FunctionContext context) {
            openCalled = true;
        }

        public @DataTypeHint("TIMESTAMP(3)") Timestamp eval(
                @DataTypeHint("TIMESTAMP(3)") TimestampData timestampData, Integer offset) {
            if (!openCalled) {
                fail("Open was not called before run.");
            }
            if (timestampData == null || offset == null) {
                return null;
            } else {
                long ts = timestampData.getMillisecond() - offset;
                int tzOffset = TimeZone.getDefault().getOffset(ts);
                return new Timestamp(ts - tzOffset);
            }
        }

        @Override
        public void close() {
            closeCalled = true;
        }
    }

    /** Testing open method is called. */
    public static class UdfWithOpen extends ScalarFunction {

        // transient make this class serializable by class name
        private transient boolean isOpened = false;

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            this.isOpened = true;
        }

        public String eval(String c) {
            if (!isOpened) {
                throw new IllegalStateException("Open method is not called!");
            }
            return "$" + c;
        }
    }

    /** Non-deterministic scalar function. */
    public static class NonDeterministicUdf extends ScalarFunction {
        Random random = new Random();

        public int eval() {
            return random.nextInt();
        }

        public int eval(int v) {
            return v + random.nextInt();
        }

        public String eval(String v) {
            return v + "-" + random.nextInt();
        }

        @Override
        public boolean isDeterministic() {
            return false;
        }
    }

    /** Test for Python Scalar Function. */
    public static class PythonScalarFunction extends ScalarFunction implements PythonFunction {
        private final String name;

        public PythonScalarFunction(String name) {
            this.name = name;
        }

        public int eval(Integer i, Integer j) {
            return i + j;
        }

        public String eval(String a) {
            return a;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public byte[] getSerializedPythonFunction() {
            return new byte[0];
        }

        @Override
        public PythonEnv getPythonEnv() {
            return new PythonEnv(PythonEnv.ExecType.PROCESS);
        }
    }

    /** Test for Python Scalar Function. */
    public static class BooleanPythonScalarFunction extends ScalarFunction
            implements PythonFunction {
        private final String name;

        public BooleanPythonScalarFunction(String name) {
            this.name = name;
        }

        public boolean eval(Integer i, Integer j) {
            return i + j > 1;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public byte[] getSerializedPythonFunction() {
            return new byte[0];
        }

        @Override
        public PythonEnv getPythonEnv() {
            return null;
        }
    }

    /** Test for Python Scalar Function. */
    public static class RowPythonScalarFunction extends ScalarFunction implements PythonFunction {

        private final String name;

        public RowPythonScalarFunction(String name) {
            this.name = name;
        }

        @DataTypeHint("ROW<f0 INT, f1 ROW<f0 INT>>")
        public Row eval(int a) {
            return Row.of(a + 1, Row.of(a * a));
        }

        @DataTypeHint("ROW<f0 INT, f1 ROW<f0 INT>>")
        public Row eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... args) {
            return Row.of(1, Row.of(2));
        }

        @Override
        public boolean takesRowAsInput() {
            return true;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public byte[] getSerializedPythonFunction() {
            return new byte[0];
        }

        @Override
        public PythonEnv getPythonEnv() {
            return null;
        }
    }

    /** Test for Python Scalar Function. */
    public static class RowJavaScalarFunction extends ScalarFunction {

        private final String name;

        public RowJavaScalarFunction(String name) {
            this.name = name;
        }

        @DataTypeHint("ROW<f0 INT, f1 ROW<f0 INT>>")
        public Row eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... a) {
            return Row.of(1, 2);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /** Test for Pandas Python Scalar Function. */
    public static class PandasScalarFunction extends PythonScalarFunction {
        public PandasScalarFunction(String name) {
            super(name);
        }

        @Override
        public PythonFunctionKind getPythonFunctionKind() {
            return PythonFunctionKind.PANDAS;
        }
    }

    /** Test for Pandas Python Scalar Function. */
    public static class BooleanPandasScalarFunction extends BooleanPythonScalarFunction {
        public BooleanPandasScalarFunction(String name) {
            super(name);
        }

        @Override
        public PythonFunctionKind getPythonFunctionKind() {
            return PythonFunctionKind.PANDAS;
        }
    }

    /** Test for Pandas Python Scalar Function. */
    public static class RowPandasScalarFunction extends RowPythonScalarFunction {

        public RowPandasScalarFunction(String name) {
            super(name);
        }

        @Override
        public PythonFunctionKind getPythonFunctionKind() {
            return PythonFunctionKind.PANDAS;
        }
    }

    /** A Python UDF that returns current timestamp with any input. */
    public static class PythonTimestampScalarFunction extends ScalarFunction
            implements PythonFunction {

        @DataTypeHint("TIMESTAMP(3)")
        public LocalDateTime eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object... o) {
            return LocalDateTime.now();
        }

        @Override
        public byte[] getSerializedPythonFunction() {
            return new byte[0];
        }

        @Override
        public PythonEnv getPythonEnv() {
            return null;
        }
    }
}
