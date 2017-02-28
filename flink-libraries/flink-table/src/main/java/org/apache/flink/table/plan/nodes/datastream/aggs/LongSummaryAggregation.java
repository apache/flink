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

import java.math.BigDecimal;

import org.apache.flink.api.java.summarize.aggregation.Aggregator;
import org.apache.flink.api.java.summarize.aggregation.NumericSummaryAggregator;

public class LongSummaryAggregation extends NumericSummaryAggregator<Long> {

	private static final long serialVersionUID = 1L;
	
	public class MinLongAggregator implements StreamAggregator<Long,Long> {

		private static final long serialVersionUID = 1L;
		private long min = Long.MAX_VALUE;

		@Override
		public void aggregate(Long value) {
			min = Math.min(min, value);
		}

		@Override
		public void combine(Aggregator<Long, Long> other) {
			min = Math.min(min,((MinLongAggregator)other).min);
		}

		@Override
		public Long result() {
			return min;
		}

		@Override
		public void reset() {
			min = Long.MAX_VALUE;
		}

		@Override
		public void evictElement(Long evictedValue) {
			
		}
	}

	public class MaxLongAggregator implements StreamAggregator<Long,Long> {

		private static final long serialVersionUID = 1L;
		private long max = Long.MIN_VALUE;

		@Override
		public void aggregate(Long value) {
			max = Math.max(max, value);
		}

		@Override
		public void combine(Aggregator<Long, Long> other) {
			max = Math.max(max, ((MaxLongAggregator) other).max);
		}

		@Override
		public Long result() {
			return max;
		}

		@Override
		public void reset() {
			max = Long.MIN_VALUE;
		}

		@Override
		public void evictElement(Long evictedValue) {
			
		}
	}

	public class SumLongAggregator implements StreamAggregator<Long,Long> {

		private static final long serialVersionUID = 1L;
		private long sum = 0L;

		@Override
		public void aggregate(Long value) {
			sum += value;
		}

		@Override
		public void combine(Aggregator<Long, Long> other) {
			sum += ((SumLongAggregator)other).sum;
		}

		@Override
		public Long result() {
			return sum;
		}

		@Override
		public void reset() {
			sum = 0L;
		}

		@Override
		public void evictElement(Long evictedValue) {
			
		}
	}


	public class AverageLongAggregator implements StreamAggregator<Long,Double>{

		private static final long serialVersionUID = 1L;
		private Long sum = 0L;
		private Long count = 0L;

		@Override
		public void aggregate(Long value) {
			sum+=value;
			count++;
		}

		@Override
		public void combine(Aggregator<Long, Double> otherSameType) {
			if(otherSameType instanceof AverageLongAggregator){
				sum+=((AverageLongAggregator)otherSameType).getSum();
				count+=((AverageLongAggregator)otherSameType).getCount();
			}else{
				throw new IllegalArgumentException("Combining average aggregators works only"
						+ " with other average aggregators. "
						+ otherSameType.getClass() +" is not!");
			}
		}

		@Override
		public Double result() {
			return new BigDecimal(sum).divide(new BigDecimal(count)).doubleValue();
		}

		@Override
		public void reset() {
			sum =0L;
			count=0L;
		}

		@Override
		public void evictElement(Long evictedValue) {
			sum-=evictedValue;
			count--;
		}
		
		public Long getCount(){
			return count;
		}
		
		public Long getSum(){
			return sum;
		}
	}
	
	@Override
	public StreamAggregator<Long, Long> initMin() {
		return new MinLongAggregator();
	}

	@Override
	public StreamAggregator<Long, Long> initMax() {
		return new MaxLongAggregator();
	}

	@Override
	public StreamAggregator<Long, Long> initSum() {
		return new SumLongAggregator();
	}
	
	public StreamAggregator<Long,Double> initAvg(){
		return new AverageLongAggregator();
	}
	
	public StreamAggregator<Object,Long> initCount(){
		return new AnyCounterAggregator();
	}

	@Override
	public boolean isNan(Long number) {
		// NaN never applies here because only types like Float and Double have NaN
		return false;
	}

	@Override
	public boolean isInfinite(Long number) {
		// Infinity never applies here because only types like Float and Double have Infinity
		return false;
	}
}
