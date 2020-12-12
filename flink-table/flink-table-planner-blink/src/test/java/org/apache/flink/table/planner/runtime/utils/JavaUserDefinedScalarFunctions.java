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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Random;
import java.util.TimeZone;

import static org.junit.Assert.fail;

/**
 * Test scalar functions.
 */
public class JavaUserDefinedScalarFunctions {

	/**
	 * Increment input.
	 */
	public static class JavaFunc0 extends ScalarFunction {
		public long eval(Long l) {
			return l + 1;
		}
	}

	/**
	 * Concatenate inputs as strings.
	 */
	public static class JavaFunc1 extends ScalarFunction {
		public String eval(Integer a, int b,  TimestampData c) {
			Long ts = (c == null) ? null : c.getMillisecond();
			return a + " and " + b + " and " + ts;
		}
	}

	/**
	 * Append product to string.
	 */
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
	 * Test overloading.
	 */
	public static class JavaFunc3 extends ScalarFunction {
		public int eval(String a, int... b) {
			return b.length;
		}

		public String eval(String c) {
			return c;
		}
	}

	/**
	 * Concatenate arrays as strings.
	 */
	public static class JavaFunc4 extends ScalarFunction {
		public String eval(Integer[] a, String[] b) {
			return Arrays.toString(a) + " and " + Arrays.toString(b);
		}
	}

	/**
	 * A UDF minus Timestamp with the specified offset.
	 * This UDF also ensures open and close are called.
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
				@DataTypeHint("TIMESTAMP(3)") TimestampData timestampData,
				Integer offset) {
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

	/**
	 * Testing open method is called.
	 */
	public static class UdfWithOpen extends ScalarFunction {

		private boolean isOpened = false;

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

	/**
	 * Non-deterministic scalar function.
	 */
	public static class NonDeterministicUdf extends ScalarFunction {
		Random random = new Random();

		public int eval() {
			return random.nextInt();
		}

		public int eval(int v) {
			return v + random.nextInt();
		}

		@Override
		public boolean isDeterministic() {
			return false;
		}
	}

	/**
	 * Test for Python Scalar Function.
	 */
	public static class PythonScalarFunction extends ScalarFunction implements PythonFunction  {
		private final String name;

		public PythonScalarFunction(String name) {
			this.name = name;
		}

		public int eval(int i, int j) {
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

	/**
	 * Test for Python Scalar Function.
	 */
	public static class BooleanPythonScalarFunction extends ScalarFunction implements PythonFunction {
		private final String name;

		public BooleanPythonScalarFunction(String name) {
			this.name = name;
		}

		public boolean eval(int i, int j) {
			return i + j > 1;
		}

		@Override
		public TypeInformation<?> getResultType(Class<?>[] signature) {
			return BasicTypeInfo.BOOLEAN_TYPE_INFO;
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

	/**
	 * Test for Python Scalar Function.
	 */
	public static class RowPythonScalarFunction extends ScalarFunction implements PythonFunction {

		private final String name;

		public RowPythonScalarFunction(String name) {
			this.name = name;
		}

		public Row eval(int a) {
			return Row.of(a + 1, Row.of(a * a));
		}

		@Override
		public TypeInformation<?> getResultType(Class<?>[] signature) {
			return Types.ROW(BasicTypeInfo.INT_TYPE_INFO, Types.ROW(BasicTypeInfo.INT_TYPE_INFO));
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

	/**
	 * Test for Pandas Python Scalar Function.
	 */
	public static class PandasScalarFunction extends PythonScalarFunction {
		public PandasScalarFunction(String name) {
			super(name);
		}

		@Override
		public PythonFunctionKind getPythonFunctionKind() {
			return PythonFunctionKind.PANDAS;
		}
	}

	/**
	 * Test for Pandas Python Scalar Function.
	 */
	public static class BooleanPandasScalarFunction extends BooleanPythonScalarFunction {
		public BooleanPandasScalarFunction(String name) {
			super(name);
		}

		@Override
		public PythonFunctionKind getPythonFunctionKind() {
			return PythonFunctionKind.PANDAS;
		}
	}
}
