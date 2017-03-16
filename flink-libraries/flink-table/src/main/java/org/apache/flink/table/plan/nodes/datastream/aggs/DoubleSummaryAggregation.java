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

import static org.apache.flink.api.java.summarize.aggregation.CompensatedSum.ZERO;

import org.apache.flink.api.java.summarize.aggregation.Aggregator;
import org.apache.flink.api.java.summarize.aggregation.CompensatedSum;
import org.apache.flink.api.java.summarize.aggregation.NumericSummaryAggregator;

public class DoubleSummaryAggregation extends NumericSummaryAggregator<Double> {

	private static final long serialVersionUID = 1L;

	public class MinDoubleAggregator implements StreamAggregator<Double,Double> {

		private static final long serialVersionUID = 1L;
		private double min = Double.MAX_VALUE;

		@Override
		public void aggregate(Double value) {
			min = Math.min(min, value);
		}

		@Override
		public void combine(Aggregator<Double, Double> other) {
			min = Math.min(min,((MinDoubleAggregator)other).min);
		}

		@Override
		public Double result() {
			return min;
		}


		@Override
		public void reset() {
			min = Double.MAX_VALUE;
		}

		@Override
		public void evictElement(Double evictedValue) {
			
		}
		
		
	}

	public class MaxDoubleAggregator implements StreamAggregator<Double,Double> {

		private static final long serialVersionUID = 1L;
		private double max = Double.MIN_VALUE;

		@Override
		public void aggregate(Double value) {
			max = Math.max(max, value);
		}

		@Override
		public void combine(Aggregator<Double, Double> other) {
			max = Math.max(max, ((MaxDoubleAggregator) other).max);
		}

		@Override
		public Double result() {
			return max;
		}

		@Override
		public void reset() {
			max = Double.MIN_VALUE;
			
		}

		@Override
		public void evictElement(Double evictedValue) {
			
		}
	}

	public class SumDoubleAggregator implements StreamAggregator<Double,Double> {

		private static final long serialVersionUID = 1L;
		private CompensatedSum sum = ZERO;

		@Override
		public void aggregate(Double value) {
			sum = sum.add(value);
		}

		@Override
		public void combine(Aggregator<Double, Double> other) {
			sum = sum.add(((SumDoubleAggregator)other).sum);
		}

		@Override
		public Double result() {
			return sum.value();
		}

		@Override
		public void reset() {
			sum = ZERO;
			
		}

		@Override
		public void evictElement(Double evictedValue) {
			
		}
	}
	
	public class AverageDoubleAggregator implements StreamAggregator<Double,Double>{

		private static final long serialVersionUID = 1L;
		private Double sum = 0d;
		private Double count = 0d;

		@Override
		public void aggregate(Double value) {
			sum+=value;
			count++;
		}

		@Override
		public void combine(Aggregator<Double, Double> otherSameType) {
			if(otherSameType instanceof AverageDoubleAggregator){
				sum+=((AverageDoubleAggregator)otherSameType).getSum();
				count+=((AverageDoubleAggregator)otherSameType).getCount();
			}else{
				throw new IllegalArgumentException("Combining average aggregators works only"
						+ " with other average aggregators. "
						+ otherSameType.getClass() +" is not!");
			}
		}

		@Override
		public Double result() {
			return sum/count;
		}

		@Override
		public void reset() {
			sum =0d;
			count=0d;
		}

		@Override
		public void evictElement(Double evictedValue) {
			sum-=evictedValue;
			count--;
		}
		
		public Double getCount(){
			return count;
		}
		
		public Double getSum(){
			return sum;
		}
	}
	
	
	
	public StreamAggregator<Double,Double> initAvg(){
		return new AverageDoubleAggregator();
	}
	
	public StreamAggregator<Object,Long> initCount(){
		return new AnyCounterAggregator();
	}
	
	@Override
	public StreamAggregator<Double, Double> initMin() {
		return new MinDoubleAggregator();
	}

	@Override
	public StreamAggregator<Double, Double> initMax() {
		return new MaxDoubleAggregator();
	}

	@Override
	public StreamAggregator<Double, Double> initSum() {
		return new SumDoubleAggregator();
	}
	

	@Override
	public boolean isNan(Double number) {
		return number.isNaN();
	}

	@Override
	public boolean isInfinite(Double number) {
		return number.isInfinite();
	}
	
}
