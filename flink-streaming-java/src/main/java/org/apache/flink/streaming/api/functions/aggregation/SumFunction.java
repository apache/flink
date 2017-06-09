/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * Internal function for summing up contents of fields. This is used with {@link SumAggregator}.
 */
@Internal
public abstract class SumFunction implements Serializable {

	private static final long serialVersionUID = 1L;

	public abstract Object add(Object o1, Object o2);

	public static SumFunction getForClass(Class<?> clazz) {

		if (clazz == Integer.class) {
			return new IntSum();
		} else if (clazz == Long.class) {
			return new LongSum();
		} else if (clazz == Short.class) {
			return new ShortSum();
		} else if (clazz == Double.class) {
			return new DoubleSum();
		} else if (clazz == Float.class) {
			return new FloatSum();
		} else if (clazz == Byte.class) {
			return new ByteSum();
		} else {
			throw new RuntimeException("DataStream cannot be summed because the class "
					+ clazz.getSimpleName() + " does not support the + operator.");
		}
	}

	static class IntSum extends SumFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public Object add(Object value1, Object value2) {
			return (Integer) value1 + (Integer) value2;
		}
	}

	static class LongSum extends SumFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public Object add(Object value1, Object value2) {
			return (Long) value1 + (Long) value2;
		}
	}

	static class DoubleSum extends SumFunction {

		private static final long serialVersionUID = 1L;

		@Override
		public Object add(Object value1, Object value2) {
			return (Double) value1 + (Double) value2;
		}
	}

	static class ShortSum extends SumFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public Object add(Object value1, Object value2) {
			return (short) ((Short) value1 + (Short) value2);
		}
	}

	static class FloatSum extends SumFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public Object add(Object value1, Object value2) {
			return (Float) value1 + (Float) value2;
		}
	}

	static class ByteSum extends SumFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public Object add(Object value1, Object value2) {
			return (byte) ((Byte) value1 + (Byte) value2);
		}
	}
}
