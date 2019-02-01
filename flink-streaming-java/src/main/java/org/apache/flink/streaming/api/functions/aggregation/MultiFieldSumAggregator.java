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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory;

import java.util.ArrayList;

/**
 * An {@link AggregationFunction} that sums up multi fields.
 *
 */
@Internal
public class MultiFieldSumAggregator<T> extends AggregationFunction<T> {

	private static final long serialVersionUID = 1L;

	private       int                                 fieldSize;
	private final ArrayList<FieldAccessor<T, Object>> fieldAccessors;
	private final ArrayList<SumFunction>              adders;
	private final TypeSerializer<T>                   serializer;
	private final boolean                             isTuple;

	public MultiFieldSumAggregator(int[] pos, TypeInformation<T> typeInfo, ExecutionConfig config) {
		fieldSize = pos.length;
		fieldAccessors = new ArrayList<>(fieldSize);
		adders = new ArrayList<>(fieldSize);
		for (int index = 0; index < fieldSize; index++) {
			FieldAccessor<T, Object> fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos[index], config);
			SumFunction adder = SumFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
			fieldAccessors.add(index, fieldAccessor);
			adders.add(index, adder);
		}

		if (typeInfo instanceof TupleTypeInfo) {
			isTuple = true;
			serializer = null;
		} else {
			isTuple = false;
			this.serializer = typeInfo.createSerializer(config);
		}
	}

	public MultiFieldSumAggregator(String[] fields, TypeInformation<T> typeInfo, ExecutionConfig config) {

		fieldSize = fields.length;
		fieldAccessors = new ArrayList<>(fieldSize);
		adders = new ArrayList<>(fieldSize);
		for (int index = 0; index < fieldSize; index++) {
			FieldAccessor<T, Object> fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, fields[index], config);
			SumFunction adder = SumFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
			fieldAccessors.add(index, fieldAccessor);
			adders.add(index, adder);
		}

		if (typeInfo instanceof TupleTypeInfo) {
			isTuple = true;
			serializer = null;
		} else {
			isTuple = false;
			this.serializer = typeInfo.createSerializer(config);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public T reduce(T value1, T value2) throws Exception {

		if (isTuple) {
			Tuple result = ((Tuple) value1).copy();
			for (int index = 0; index < fieldSize; index++) {
				FieldAccessor<T, Object> fieldAccessor = fieldAccessors.get(index);
				SumFunction adder = adders.get(index);
				result = (Tuple) fieldAccessor.set((T) result, adder.add(fieldAccessor.get(value1), fieldAccessor.get(value2)));
			}
			return (T) result;
		} else {
			T result = serializer.copy(value1);
			for (int index = 0; index < fieldSize; index++) {
				FieldAccessor<T, Object> fieldAccessor = fieldAccessors.get(index);
				SumFunction adder = adders.get(index);
				result = fieldAccessor.set(result, adder.add(fieldAccessor.get(value1), fieldAccessor.get(value2)));
			}
			return result;
		}
	}
}
