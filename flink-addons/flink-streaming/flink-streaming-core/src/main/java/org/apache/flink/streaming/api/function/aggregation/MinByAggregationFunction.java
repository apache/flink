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

public class MinByAggregationFunction<T> extends ComparableAggregationFunction<T> {

	private static final long serialVersionUID = 1L;
	protected boolean first;

	public MinByAggregationFunction(int pos, boolean first) {
		super(pos);
		this.first = first;
	}

	@Override
	public <R> void compare(Tuple tuple1, Tuple tuple2) throws InstantiationException,
			IllegalAccessException {

		Comparable<R> o1 = tuple1.getField(position);
		R o2 = tuple2.getField(position);

		if (isExtremal(o1, o2)) {
			returnTuple = tuple1;
		} else {
			returnTuple = tuple2;
		}
	}

	@Override
	public <R> boolean isExtremal(Comparable<R> o1, R o2) {
		if (first) {
			return o1.compareTo(o2) <= 0;
		} else {
			return o1.compareTo(o2) < 0;
		}

	}
}
