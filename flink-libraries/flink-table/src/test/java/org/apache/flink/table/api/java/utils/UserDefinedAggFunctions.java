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
package org.apache.flink.table.api.java.utils;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.RichAggregateFunction;

import java.io.IOException;
import java.util.Iterator;

public class UserDefinedAggFunctions {

    // Accumulator for WeightedAvg
    public static class WeightedAvgAccum extends Tuple2<Long, Integer> {
        public long sum = 0;
        public int count = 0;
    }

    // Base class for WeightedAvg
    public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccum> {
        @Override
        public WeightedAvgAccum createAccumulator() {
            return new WeightedAvgAccum();
        }

        @Override
        public Long getValue(WeightedAvgAccum accumulator) {
            if (accumulator.count == 0)
                return null;
            else
                return accumulator.sum/accumulator.count;
        }

        //Overloaded accumulate method
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

    // A WeightedAvg class with merge method
    public static class WeightedAvgWithMerge extends WeightedAvg {
        public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
            Iterator<WeightedAvgAccum> iter = it.iterator();
            while (iter.hasNext()) {
                WeightedAvgAccum a = iter.next();
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }
    }

    // A WeightedAvg class with merge and reset method
    public static class WeightedAvgWithMergeAndReset extends WeightedAvgWithMerge {
        public void resetAccumulator(WeightedAvgAccum acc) {
            acc.count = 0;
            acc.sum = 0L;
        }
    }

    // A WeightedAvg class with retract method
    public static class WeightedAvgWithRetract extends WeightedAvg {
        //Overloaded retract method
        public void retract(WeightedAvgAccum accumulator, long iValue, int iWeight) {
            accumulator.sum -= iValue * iWeight;
            accumulator.count -= iWeight;
        }

        //Overloaded retract method
        public void retract(WeightedAvgAccum accumulator, int iValue, int iWeight) {
            accumulator.sum -= iValue * iWeight;
            accumulator.count -= iWeight;
        }
    }

	// A WeightedAvg class with retract and reset method
    public static class WeightedAvgWithRetractAndReset extends WeightedAvgWithRetract {
		public void resetAccumulator(WeightedAvgAccum acc) {
			acc.count = 0;
			acc.sum = 0L;
		}
	}

	// Accumulator for WeightedAvg with state
	public static class WeightedStateAvgAccum {
		final String valueName = "valuestate";
	}

	// Base class for WeightedAvg with state
	public static class WeightedStateAvg
		extends RichAggregateFunction<Long, WeightedStateAvgAccum> {

		@Override
		public WeightedStateAvgAccum createAccumulator() {
			WeightedStateAvgAccum accum = new WeightedStateAvgAccum();
			registerValue(accum.valueName, new TupleTypeInfo<>(Types.LONG, Types.INT));

			try {
				getValueByStateName(accum.valueName).update(new Tuple2<>(0L, 0));
			} catch (IOException e) {
				throw new RuntimeException("init accumulator value failed!", e);
			}
			return accum;
		}

		@Override
		public Long getValue(WeightedStateAvgAccum accumulator) {
			try {
				Tuple2<Long, Integer> avgPair =
					(Tuple2<Long, Integer>) getValueByStateName(accumulator.valueName).value();
				if (avgPair.f1 == 0)
					return null;
				else
					return avgPair.f0 / avgPair.f1;
			} catch (IOException e) {
				throw new RuntimeException("getValue failed!", e);
			}
		}

		public void accumulate(WeightedStateAvgAccum accumulator, long iValue, int iWeight) {
			try {
				Tuple2<Long, Integer> avgPair =
					(Tuple2<Long, Integer>) getValueByStateName(accumulator.valueName).value();
				avgPair.f0 += iValue * iWeight;
				avgPair.f1 += iWeight;
				getValueByStateName(accumulator.valueName).update(avgPair);
			} catch (IOException e) {
				throw new RuntimeException("accumulate failed!", e);
			}
		}
	}

	// A WeightedStateAvg class with retract method
	public static class WeightedStateAvgWithRetract extends WeightedStateAvg {
		//Overloaded retract method
		public void retract(WeightedStateAvgAccum accumulator, long iValue, int iWeight) {
			try {
				Tuple2<Long, Integer> avgPair =
					(Tuple2<Long, Integer>) getValueByStateName(accumulator.valueName).value();
				avgPair.f0 -= iValue * iWeight;
				avgPair.f1 -= iWeight;
				getValueByStateName(accumulator.valueName).update(avgPair);
			} catch (IOException e) {
				throw new RuntimeException("retract failed!", e);
			}
		}
	}

	// A WeightedStateAvg class with retract and reset method
	public static class WeightedStateAvgWithRetractAndReset extends WeightedStateAvgWithRetract {
		public void resetAccumulator(WeightedStateAvgAccum acc) {
			try {
				Tuple2<Long, Integer> avgPair =
					(Tuple2<Long, Integer>) getValueByStateName(acc.valueName).value();
				avgPair.f0 = 0L;
				avgPair.f1 = 0;
				getValueByStateName(acc.valueName).update(avgPair);
			} catch (IOException e) {
				throw new RuntimeException("retract failed!", e);
			}
		}
	}
}
