/**
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

package org.apache.flink.streaming.api.function.aggregation;

import org.apache.flink.api.java.tuple.Tuple;

public class StreamingSumAggregationFunction<T> extends StreamingAggregationFunction<T> {

	private static final long serialVersionUID = 1L;

	public StreamingSumAggregationFunction(int pos) {
		super(pos);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T reduce(T value1, T value2) throws Exception {
		if (value1 instanceof Tuple) {
			Tuple tuple1 = (Tuple) value1;
			Tuple tuple2 = (Tuple) value2;

			copyTuple(tuple2);
			returnTuple.setField(add(tuple1.getField(position), tuple2.getField(position)), position);

			return (T) returnTuple;
		} else {
			return (T) add(value1, value2);
		}
	}

	private Object add(Object value1, Object value2) {
		if (value1 instanceof Integer) {
			return (Integer) value1 + (Integer) value2;
		} else if (value1 instanceof Double) {
			return (Double) value1 + (Double) value2;
		} else if (value1 instanceof Float) {
			return (Float) value1 + (Float) value2;
		} else if (value1 instanceof Long) {
			return (Long) value1 + (Long) value2;
		} else if (value1 instanceof Short) {
			return (short) ((Short) value1 + (Short) value2);
		} else if (value1 instanceof Byte) {
			return (byte) ((Byte) value1 + (Byte) value2);
		} else {
			throw new RuntimeException("DataStream cannot be summed because the class "
					+ value1.getClass().getSimpleName() + " does not support the + operator.");
		}
	}
}
