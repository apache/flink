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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.dataformat.SqlTimestamp;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.table.functions.python.PythonFunction;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Random;
import java.util.TimeZone;

import static org.junit.Assert.fail;

/**
 * Test scalar functions.
 */
public class JavaUserDefinedScalarFunctions {

	/**
	 * Accumulator for test requiresOver.
	 */
	public static class AccumulatorOver extends Tuple2<Long, Integer> {}

	/**
	 * Test for requiresOver.
	 */
	public static class OverAgg0 extends AggregateFunction<Long, AccumulatorOver> {
		@Override
		public AccumulatorOver createAccumulator() {
			return new AccumulatorOver();
		}

		@Override
		public Long getValue(AccumulatorOver accumulator) {
			return 1L;
		}

		//Overloaded accumulate method
		public void accumulate(AccumulatorOver accumulator, long iValue, int iWeight) {
		}

		@Override
		public boolean requiresOver() {
			return true;
		}
	}

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
		public String eval(Integer a, int b,  SqlTimestamp c) {
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

		public Timestamp eval(SqlTimestamp sqlTimestamp, Integer offset) {
			if (!openCalled) {
				fail("Open was not called before run.");
			}
			if (sqlTimestamp == null || offset == null) {
				return null;
			} else {
				long ts = sqlTimestamp.getMillisecond() - offset;
				int tzOffset = TimeZone.getDefault().getOffset(ts);
				return new Timestamp(ts - tzOffset);
			}
		}

		@Override
		public TypeInformation<?> getResultType(Class<?>[] signature) {
			return Types.SQL_TIMESTAMP;
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

		@Override
		public TypeInformation<?> getResultType(Class<?>[] signature) {
			return BasicTypeInfo.INT_TYPE_INFO;
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
}
