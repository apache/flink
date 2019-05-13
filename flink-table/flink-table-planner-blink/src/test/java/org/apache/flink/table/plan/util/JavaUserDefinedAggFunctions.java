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

package org.apache.flink.table.plan.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

/**
 * Test aggregator functions.
 */
public class JavaUserDefinedAggFunctions {

	/**
	 * Accumulator of VarSumAggFunction.
	 */
	public static class VarSumAcc {
		public long sum;
	}

	/**
	 * Only used for test.
	 */
	public static class VarSumAggFunction extends AggregateFunction<Long, VarSumAcc> {

		@Override
		public VarSumAcc createAccumulator() {
			return new VarSumAcc();
		}

		public void accumulate(VarSumAcc acc, Integer ...args) {
			for (Integer x : args) {
				if (x != null) {
					acc.sum += x.longValue();
				}
			}
		}

		@Override
		public Long getValue(VarSumAcc accumulator) {
			return accumulator.sum;
		}
	}

	/**
	 * Only used for test.
	 * The difference between the class and VarSumAggFunction is accumulator type.
	 */
	public static class VarSum1AggFunction extends AggregateFunction<Long, VarSumAcc> {

		@Override
		public VarSumAcc createAccumulator() {
			return new VarSumAcc();
		}

		public void accumulate(VarSumAcc acc, Integer... args) {
			for (Integer x : args) {
				if (x != null) {
					acc.sum += x.longValue();
				}
			}
		}

		@Override
		public Long getValue(VarSumAcc accumulator) {
			return accumulator.sum;
		}

		@Override
		public TypeInformation<Long> getResultType() {
			return Types.LONG;
		}
	}

	/**
	 * Only used for test.
	 * The difference between the class and VarSumAggFunction is accumulator type.
	 */
	public static class VarSum2AggFunction extends AggregateFunction<Long, Long> {

		@Override
		public Long createAccumulator() {
			return 0L;
		}

		public void accumulate(Long acc, Integer... args) {
			for (Integer x : args) {
				if (x != null) {
					acc += x.longValue();
				}
			}
		}

		@Override
		public Long getValue(Long accumulator) {
			return accumulator;
		}

		@Override
		public TypeInformation<Long> getResultType() {
			return Types.LONG;
		}
	}

	/**
	 * Accumulator for WeightedAvg.
	 */
	public static class WeightedAvgAccum {
		public long sum = 0;
		public int count = 0;
	}

	/**
	 * Base class for WeightedAvg.
	 */
	public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccum> {
		@Override
		public WeightedAvgAccum createAccumulator() {
			return new WeightedAvgAccum();
		}

		@Override
		public Long getValue(WeightedAvgAccum accumulator) {
			if (accumulator.count == 0) {
				return null;
			} else {
				return accumulator.sum / accumulator.count;
			}
		}

		// overloaded accumulate method
		// dummy to test constants
		public void accumulate(WeightedAvgAccum accumulator, long iValue, int iWeight, int x, String string) {
			accumulator.sum += (iValue + Integer.parseInt(string)) * iWeight;
			accumulator.count += iWeight;
		}

		// overloaded accumulate method
		public void accumulate(WeightedAvgAccum accumulator, long iValue, int iWeight) {
			accumulator.sum += iValue * iWeight;
			accumulator.count += iWeight;
		}

		//Overloaded accumulate method
		public void accumulate(WeightedAvgAccum accumulator, int iValue, int iWeight) {
			accumulator.sum += iValue * iWeight;
			accumulator.count += iWeight;
		}
	}

	/**
	 * A WeightedAvg class with merge method.
	 */
	public static class WeightedAvgWithMerge extends WeightedAvg {
		public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
			Iterator<WeightedAvgAccum> iter = it.iterator();
			while (iter.hasNext()) {
				WeightedAvgAccum a = iter.next();
				acc.count += a.count;
				acc.sum += a.sum;
			}
		}

		@Override
		public String toString() {
			return "myWeightedAvg";
		}
	}

	/**
	 * A WeightedAvg class with merge and reset method.
	 */
	public static class WeightedAvgWithMergeAndReset extends WeightedAvgWithMerge {
		public void resetAccumulator(WeightedAvgAccum acc) {
			acc.count = 0;
			acc.sum = 0L;
		}
	}
}
