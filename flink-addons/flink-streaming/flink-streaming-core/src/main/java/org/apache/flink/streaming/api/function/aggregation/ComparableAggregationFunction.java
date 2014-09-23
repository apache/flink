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

package org.apache.flink.streaming.api.function.aggregation;

import org.apache.flink.api.java.tuple.Tuple;

public abstract class ComparableAggregationFunction<T> extends AggregationFunction<T> {

	private static final long serialVersionUID = 1L;

	public ComparableAggregationFunction(int positionToAggregate) {
		super(positionToAggregate);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T reduce(T value1, T value2) throws Exception {
		if (value1 instanceof Tuple) {
			Tuple t1 = (Tuple) value1;
			Tuple t2 = (Tuple) value2;

			compare(t1, t2);

			return (T) returnTuple;
		} else if (value1 instanceof Comparable) {
			if (isExtremal((Comparable<Object>) value1, value2)) {
				return value1;
			}else{
				return value2;
			}
		} else {
			throw new RuntimeException("The values " + value1 +  " and "+ value2 + " cannot be compared.");
		}
	}

	public <R> void compare(Tuple tuple1, Tuple tuple2) throws InstantiationException,
			IllegalAccessException {

		Comparable<R> o1 = tuple1.getField(position);
		R o2 = tuple2.getField(position);

		if (isExtremal(o1, o2)) {
			tuple2.setField(o1, position);
		}
		returnTuple = tuple2;
	}

	public abstract <R> boolean isExtremal(Comparable<R> o1, R o2);
}
