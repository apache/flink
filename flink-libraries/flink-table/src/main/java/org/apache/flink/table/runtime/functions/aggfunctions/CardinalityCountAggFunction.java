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

package org.apache.flink.table.runtime.functions.aggfunctions;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.functions.aggfunctions.cardinality.CardinalityCountAccumulator;
import org.apache.flink.table.runtime.functions.aggfunctions.cardinality.HyperLogLog;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Built-in cardinality count aggregate function.
 */
public class CardinalityCountAggFunction
		extends AggregateFunction<Long, CardinalityCountAccumulator> {
	@Override
	public CardinalityCountAccumulator createAccumulator() {
		return new CardinalityCountAccumulator();
	}

	public void accumulate(CardinalityCountAccumulator acc, Double rsd, Object value) {
		if (acc.f0 == null) {
			if (rsd <= 0.0 || rsd >= 1.0) {
				throw new IllegalArgumentException(
						"Relative standard deviation should be > 0.0 and < 1.0, but get [" + rsd + "].");
			}
			acc.f0 = rsd;
			try {
				HyperLogLog ce = new HyperLogLog(acc.f0);
				acc.f1 = ce.getBytes();
			} catch (IOException e) {
				throw new RuntimeException(e.getCause());
			}
		}

		if (value != null) {
			try {
				HyperLogLog ce = HyperLogLog.Builder.build(acc.f1);
				ce.offer(value);
				acc.f1 = ce.getBytes();
			} catch (IOException e) {
				throw new RuntimeException(e.getCause());
			}
		}
	}

	/**
	 * Calcite resolve the 0.01 as BigDecimal.
	 */
	public void accumulate(CardinalityCountAccumulator acc, BigDecimal rsd, Object value) {
		accumulate(acc, rsd.doubleValue(), value);
	}

	/**
	 * Cardinality count is approximately statistical algorithm, we can ignore the
	 * retract record.
	 */
	public void retract(Tuple1<byte[]> acc, Object value) {
		// do nothing, ignore the retract record.
	}

	public void resetAccumulator(CardinalityCountAccumulator acc) {
		acc.f0 = null;
		acc.f1 = null;
	}

	public Long getValue(CardinalityCountAccumulator acc) {
		try {
			if (acc.f0 == null) {
				return 0L;
			}
			return HyperLogLog.Builder.build(acc.f1).cardinality();
		} catch (IOException e) {
			throw new RuntimeException(e.getCause());
		}
	}

}
