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
package org.apache.flink.table.plan.nodes.datastream.aggs;

import org.apache.flink.api.java.summarize.aggregation.Aggregator;
import org.apache.flink.api.java.summarize.aggregation.NumericSummaryAggregator;

public class IntegerSummaryAggregation extends NumericSummaryAggregator<Integer> {

	private static final long serialVersionUID = 6630407882798978778L;

	public class MinIntegerAggregator implements StreamAggregator<Integer, Integer> {

		private static final long serialVersionUID = -3576239746558269747L;
		private int min = Integer.MAX_VALUE;

		@Override
		public void aggregate(Integer value) {
			min = Math.min(min, value);
		}

		@Override
		public void combine(Aggregator<Integer, Integer> other) {
			min = Math.min(min, ((MinIntegerAggregator) other).min);
		}

		@Override
		public Integer result() {
			return min;
		}

		@Override
		public void reset() {
			min = Integer.MAX_VALUE;
		}

		@Override
		public void evictElement(Integer evictedValue) {
			
		}
	}

	public class MaxIntegerAggregator implements StreamAggregator<Integer, Integer> {

		private static final long serialVersionUID = -8450072340111715884L;
		private int max = Integer.MIN_VALUE;

		@Override
		public void aggregate(Integer value) {
			max = Math.max(max, value);
		}

		@Override
		public void combine(Aggregator<Integer, Integer> other) {
			max = Math.max(max, ((MaxIntegerAggregator) other).max);
		}

		@Override
		public Integer result() {
			return max;
		}

		@Override
		public void reset() {
			max = Integer.MIN_VALUE;
		}

		@Override
		public void evictElement(Integer evictedValue) {
			
		}
	}

	public class SumIntegerAggregator implements StreamAggregator<Integer, Integer> {

		private static final long serialVersionUID = 7802173071935493176L;
		private int sum = 0;

		@Override
		public void aggregate(Integer value) {
			sum += value;
		}

		@Override
		public void combine(Aggregator<Integer, Integer> other) {
			sum += ((SumIntegerAggregator) other).sum;
		}

		@Override
		public Integer result() {
			return sum;
		}

		@Override
		public void reset() {
			sum = 0;
			
		}

		@Override
		public void evictElement(Integer evictedValue) {
			
		}
	}

	public class AverageIntegerAggregator implements StreamAggregator<Integer,Double>{

		private static final long serialVersionUID = 1L;
		private Integer sum = 0;
		private Integer count = 0;

		@Override
		public void aggregate(Integer value) {
			sum+=value;
			count++;
		}

		@Override
		public void combine(Aggregator<Integer, Double> otherSameType) {
			if(otherSameType instanceof AverageIntegerAggregator){
				sum+=((AverageIntegerAggregator)otherSameType).getSum();
				count+=((AverageIntegerAggregator)otherSameType).getCount();
			}else{
				throw new IllegalArgumentException("Combining average aggregators works only"
						+ " with other average aggregators. "
						+ otherSameType.getClass() +" is not!");
			}
		}

		@Override
		public Double result() {
			return new Double(sum)/new Double(count);
		}

		@Override
		public void reset() {
			sum =0;
			count=0;
		}

		@Override
		public void evictElement(Integer evictedValue) {
			sum-=evictedValue;
			count--;
		}
		
		public Integer getCount(){
			return count;
		}
		
		public Integer getSum(){
			return sum;
		}
	}
	

	@Override
	public StreamAggregator<Integer, Integer> initMin() {
		return new MinIntegerAggregator();
	}

	@Override
	public StreamAggregator<Integer, Integer> initMax() {
		return new MaxIntegerAggregator();
	}

	@Override
	public StreamAggregator<Integer, Integer> initSum() {
		return new SumIntegerAggregator();
	}
	
	public StreamAggregator<Integer,Double> initAvg(){
		return new AverageIntegerAggregator();
	}
	
	public StreamAggregator<Object,Long> initCount(){
		return new AnyCounterAggregator();
	}

	@Override
	public boolean isNan(Integer number) {
		// NaN never applies here because only types like Float and Double have
		// NaN
		return false;
	}

	@Override
	public boolean isInfinite(Integer number) {
		// Infinity never applies here because only types like Float and Double
		// have Infinity
		return false;
	}

	
}
