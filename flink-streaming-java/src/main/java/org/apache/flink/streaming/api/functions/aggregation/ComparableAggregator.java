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

public class ComparableAggregator<T> extends AggregationFunction<T> {

	private static final long serialVersionUID = 1L;

	public Comparator comparator;
	public boolean byAggregate;
	public boolean first;
	FieldAccessor<T, Object> fieldAccessor;
	
	private ComparableAggregator(int pos, AggregationType aggregationType, boolean first) {
		super(pos);
		this.comparator = Comparator.getForAggregation(aggregationType);
		this.byAggregate = (aggregationType == AggregationType.MAXBY) || (aggregationType == AggregationType.MINBY);
		this.first = first;
	}

	public ComparableAggregator(int positionToAggregate, TypeInformation<T> typeInfo, AggregationType aggregationType
			, ExecutionConfig config) {
		this(positionToAggregate, typeInfo, aggregationType, false, config);
	}

	public ComparableAggregator(int positionToAggregate, TypeInformation<T> typeInfo, AggregationType aggregationType,
								boolean first, ExecutionConfig config) {
		this(positionToAggregate, aggregationType, first);
		this.fieldAccessor = FieldAccessor.create(positionToAggregate, typeInfo, config);
		this.first = first;
	}

	public ComparableAggregator(String field,
			TypeInformation<T> typeInfo, AggregationType aggregationType, boolean first, ExecutionConfig config) {
		this(0, aggregationType, first);
		this.fieldAccessor = FieldAccessor.create(field, typeInfo, config);
		this.first = first;
	}


	@SuppressWarnings("unchecked")
	@Override
	public T reduce(T value1, T value2) throws Exception {
		Comparable<Object> o1 = (Comparable<Object>) fieldAccessor.get(value1);
		Object o2 = fieldAccessor.get(value2);

		int c = comparator.isExtremal(o1, o2);

		if (byAggregate) {
			if (c == 1) {
				return value1;
			}
			if (first) {
				if (c == 0) {
					return value1;
				}
			}

			return value2;

		} else {
			if (c == 0) {
				value1 = fieldAccessor.set(value1, o2);
			}
			return value1;
		}

	}

}
