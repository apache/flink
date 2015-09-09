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

package org.apache.flink.contrib.operatorstatistics;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.cardinality.LinearCounting;
import org.apache.flink.contrib.operatorstatistics.heavyhitters.HeavyHitter;
import org.apache.flink.contrib.operatorstatistics.heavyhitters.LossyCounting;
import org.apache.flink.contrib.operatorstatistics.heavyhitters.CountMinHeavyHitter;
import org.apache.flink.contrib.operatorstatistics.heavyhitters.HeavyHitterMergeException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

/**
 * Data structure that encapsulates statistical information of data that has only been processed by one pass
 * This statistical information is meant to help determine the distribution of the data that has been processed
 * in an operator so that we can determine if it is necessary to repartition the data
 *
 * The statistics to be gathered are configurable and represented by a {@link OperatorStatisticsConfig} object.
 *
 * The information encapsulated in this class is min, max, a structure enabling estimation of count distinct and a
 * structure holding the heavy hitters along with their frequency.
 *
 */
public class OperatorStatistics implements Serializable {

	OperatorStatisticsConfig config;

	transient Object min = null;
	transient Object max = null;
	long cardinality = 0;
	transient ICardinality countDistinct;
	transient HeavyHitter heavyHitter;

	public OperatorStatistics(OperatorStatisticsConfig config) {
		this.config = config;
		if (config.collectCountDistinct) {
			if (config.countDistinctAlgorithm.equals(OperatorStatisticsConfig.CountDistinctAlgorithm.LINEAR_COUNTING)) {
				countDistinct = new LinearCounting(config.getCountDbitmap());
			}
			if (config.countDistinctAlgorithm.equals(OperatorStatisticsConfig.CountDistinctAlgorithm.HYPERLOGLOG)) {
				countDistinct = new HyperLogLog(config.getCountDlog2m());
			}
		}
		if (config.collectHeavyHitters) {
			if (config.heavyHitterAlgorithm.equals(OperatorStatisticsConfig.HeavyHitterAlgorithm.LOSSY_COUNTING)) {
				heavyHitter =
						new LossyCounting(config.getHeavyHitterFraction(), config.getHeavyHitterError());
			}
			if (config.heavyHitterAlgorithm.equals(OperatorStatisticsConfig.HeavyHitterAlgorithm.COUNT_MIN_SKETCH)) {
				heavyHitter =
						new CountMinHeavyHitter(config.getHeavyHitterFraction(),
								config.getHeavyHitterError(),
								config.getHeavyHitterConfidence(),
								config.getHeavyHitterSeed());
			}
		}
	}

	public void process(Object tupleObject){
		if (tupleObject instanceof Comparable) {
			if (config.collectMin && (min == null || ((Comparable) tupleObject).compareTo(min) < 0)) {
					min = tupleObject;
			}
			if (config.collectMax && (max == null || ((Comparable) tupleObject).compareTo(max) > 0)) {
					max = tupleObject;
			}
		}
		if (config.collectCountDistinct){
			countDistinct.offer(tupleObject);
		}
		if (config.collectHeavyHitters){
			heavyHitter.addObject(tupleObject);
		}
		cardinality+=1;
	}

	public void merge(OperatorStatistics other) throws RuntimeException {
		if (config.collectMin & other.min!=null && (min==null || ((Comparable) other.min).compareTo(min) < 0)) {
			this.min = other.min;
		}

		if (config.collectMax && other.max!=null && (max == null || ((Comparable) other.max).compareTo(max) > 0)) {
				this.max = other.max;
		}
		if (config.collectCountDistinct){
			try {
				ICardinality mergedCountDistinct = this.countDistinct.merge(new ICardinality[]{this.countDistinct,other.countDistinct});
				this.countDistinct = mergedCountDistinct;
			} catch (CardinalityMergeException e) {
				throw new RuntimeException("Error merging count distinct structures",e);
			}
		}
		if (config.collectHeavyHitters){
			try {
				this.heavyHitter.merge(other.heavyHitter);
			} catch (HeavyHitterMergeException e) {
				throw new RuntimeException("Error merging heavy hitter structures",e);
			}
		}
		this.cardinality+=other.cardinality;
	}

	public Object getMin() {
		return min;
	}

	public Object getMax() {
		return max;
	}

	public long estimateCountDistinct(){
		return countDistinct.cardinality();
	}

	public Map<Object,Long> getHeavyHitters(){
		return heavyHitter.getHeavyHitters();
	}

	@Override
	public String toString(){
		String out = "\ntotal cardinality: "+this.cardinality;
		if (config.collectMax) {
			out += "\nmax: " + this.max;
		}
		if (config.collectMin){
			out+="\nmin: "+this.min;
		}
		if (config.collectCountDistinct){
			if (config.countDistinctAlgorithm.equals(OperatorStatisticsConfig.CountDistinctAlgorithm.HYPERLOGLOG)){
				out+="\ncount distinct estimate("+config.countDistinctAlgorithm+
						"["+config.getCountDlog2m()+"]): "+
						this.countDistinct.cardinality();
			}else{
				out+="\ncount distinct estimate("+config.countDistinctAlgorithm+
						"["+config.getCountDbitmap()+"]): "+
						this.countDistinct.cardinality();
			}
		}
		if (config.collectHeavyHitters){
			if (config.countDistinctAlgorithm.equals(OperatorStatisticsConfig.HeavyHitterAlgorithm.LOSSY_COUNTING)) {
				out += "\nheavy hitters (" + config.heavyHitterAlgorithm +
						"[" + config.getHeavyHitterFraction() + ", "
							+ config.getHeavyHitterError() + "]):";
				out += "\n" + heavyHitter.toString();
			}else {
				out += "\nheavy hitters (" + config.heavyHitterAlgorithm +
						"[" + config.getHeavyHitterFraction() + ", "
							+ config.getHeavyHitterError() + ", "
							+ config.getHeavyHitterConfidence() + "]):";
				out += "\n" + heavyHitter.toString();
			}
		}
		return out;
	}

	@Override
	public OperatorStatistics clone(){
		OperatorStatistics clone = new OperatorStatistics(config);
		clone.min = min;
		clone.max = max;
		clone.cardinality = cardinality;

		try {
			if (config.collectCountDistinct) {
				ICardinality copy;
				if (countDistinct instanceof LinearCounting) {
					copy = new LinearCounting(config.getCountDbitmap());
				} else if (countDistinct instanceof HyperLogLog) {
					copy = new HyperLogLog(config.getCountDlog2m());
				} else {
					throw new IllegalStateException("Unsupported count distinct counter.");
				}
				clone.countDistinct = copy.merge(countDistinct);
			}
		} catch (CardinalityMergeException e) {
			throw new RuntimeException("Faild to clone OperatorStatistics!");
		}

		try {
			if (config.collectHeavyHitters) {
				HeavyHitter copy;
				if (heavyHitter instanceof LossyCounting) {
					copy = new LossyCounting(config.getHeavyHitterFraction(), config.getHeavyHitterError());
				} else if (heavyHitter instanceof CountMinHeavyHitter) {
					copy = new CountMinHeavyHitter(config.getHeavyHitterFraction(),
							config.getHeavyHitterError(),
							config.getHeavyHitterConfidence(),
							config.getHeavyHitterSeed());
				} else {
					throw new IllegalStateException("Unsupported heavy hitter counter.");
				}
				copy.merge(heavyHitter);
				clone.heavyHitter = copy;
			}
		} catch (HeavyHitterMergeException e) {
			throw new RuntimeException("Failed to clone OperatorStatistics!");
		}

		return clone;
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
		if (config.collectMin){
			out.writeObject(min);
		}
		if (config.collectMax){
			out.writeObject(max);
		}
		if (config.collectCountDistinct){
			if (config.countDistinctAlgorithm.equals(OperatorStatisticsConfig.CountDistinctAlgorithm.LINEAR_COUNTING)){
				out.writeObject(countDistinct.getBytes());
			}else{
				out.writeObject(countDistinct);
			}
		}
		if (config.collectHeavyHitters){
			out.writeObject(heavyHitter);
		}
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		if (config.collectMin){
			min = in.readObject();
		}
		if (config.collectMax){
			max = in.readObject();
		}
		if (config.collectCountDistinct){
			if (config.countDistinctAlgorithm.equals(OperatorStatisticsConfig.CountDistinctAlgorithm.LINEAR_COUNTING)){
				countDistinct = new LinearCounting((byte[])in.readObject());
			}else{
				countDistinct = (HyperLogLog)in.readObject();
			}
		}
		if (config.collectHeavyHitters) {
			if (config.heavyHitterAlgorithm.equals(OperatorStatisticsConfig.HeavyHitterAlgorithm.LOSSY_COUNTING)){
				heavyHitter = (LossyCounting)in.readObject();
			}else{
				heavyHitter = (CountMinHeavyHitter)in.readObject();
			}
		}
	}

}