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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.FieldAccessor;

public class SumAggregator<T> extends AggregationFunction<T> {

	private static final long serialVersionUID = 1L;

	FieldAccessor<T, Object> fieldAccessor;
	SumFunction adder;

	public SumAggregator(int pos, TypeInformation<T> typeInfo, ExecutionConfig config) {
		super(pos);
		fieldAccessor = FieldAccessor.create(pos, typeInfo, config);
		adder = SumFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
	}

	public SumAggregator(String field, TypeInformation<T> typeInfo, ExecutionConfig config) {
		super(0);
		fieldAccessor = FieldAccessor.create(field, typeInfo, config);
		adder = SumFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
	}

	@SuppressWarnings("unchecked")
	@Override
	public T reduce(T value1, T value2) throws Exception {
		return fieldAccessor.set(value1, adder.add(fieldAccessor.get(value1), fieldAccessor.get(value2)));
	}
}
