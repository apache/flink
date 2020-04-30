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

package org.apache.flink.table.planner.codegen.agg;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * Test avg agg function.
 */
public class TestLongAvgFunc extends AggregateFunction<Long, Tuple2<Long, Long>> {

	@Override
	public Tuple2<Long, Long> createAccumulator() {
		return new Tuple2<>(0L, 0L);
	}

	public void accumulate(Tuple2<Long, Long> acc, Long value) {
		if (value != null) {
			acc.f0 += value;
			acc.f1 += 1L;
		}
	}

	public void retract(Tuple2<Long, Long> acc, Long value) {
		if (value != null) {
			acc.f0 -= value;
			acc.f1 -= 1L;
		}
	}

	public void merge(Tuple2<Long, Long> acc, Iterable<Tuple2<Long, Long>> iterable) {
		for (Tuple2<Long, Long> a : iterable) {
			acc.f1 += a.f1;
			acc.f0 += a.f0;
		}
	}

	@Override
	public Long getValue(Tuple2<Long, Long> acc) {
		if (acc.f1 == 0) {
			return null;
		} else {
			return acc.f0 / acc.f1;
		}
	}

	@Override
	public TypeInformation<Tuple2<Long, Long>> getAccumulatorType() {
		return new TupleTypeInfo(
				createAccumulator().getClass(),
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO);
	}
}
