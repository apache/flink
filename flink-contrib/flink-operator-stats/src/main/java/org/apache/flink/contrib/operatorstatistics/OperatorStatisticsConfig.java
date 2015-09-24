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

import java.io.Serializable;

/**
 * Configures the behavior of an {@link OperatorStatistics} instance.
 *
 * Sets the parameters that determine the accuracy of the count distinct and heavy hitter sketches
 *
 * Defines the statistics to be collected. A boolean field indicates whether a given statistic should be collected or not
 *
 * Encapsulates an enum indicating which sketch should be used for count distinct and another indicating which sketch
 * should be used for detecting heavy hitters.
 */

public class OperatorStatisticsConfig implements Serializable {

	private int countDbitmap = 1000000;
	private int countDlog2m = 10;

	private int heavyHitterSeed = 121311332;
	private double heavyHitterConfidence = 0.99;
	private double heavyHitterFraction = 0.05;
	private double heavyHitterError = 0.0005;

	public boolean collectMin;
	public boolean collectMax;
	public boolean collectCountDistinct;
	public boolean collectHeavyHitters;
	public CountDistinctAlgorithm countDistinctAlgorithm;
	public HeavyHitterAlgorithm heavyHitterAlgorithm;

	public OperatorStatisticsConfig(){
		this.collectMin = true;
		this.collectMax = true;
		this.collectCountDistinct = true;
		this.collectHeavyHitters = true;
		this.countDistinctAlgorithm = CountDistinctAlgorithm.HYPERLOGLOG;
		this.heavyHitterAlgorithm = HeavyHitterAlgorithm.LOSSY_COUNTING;
	}

	public OperatorStatisticsConfig(boolean collect){
		this.collectMin = collect;
		this.collectMax = collect;
		this.collectCountDistinct = collect;
		this.collectHeavyHitters = collect;
		this.countDistinctAlgorithm = CountDistinctAlgorithm.HYPERLOGLOG; //Defaut algorithm
		this.heavyHitterAlgorithm = HeavyHitterAlgorithm.LOSSY_COUNTING; //Default algorithm
	}

	public OperatorStatisticsConfig(CountDistinctAlgorithm countDistinct, HeavyHitterAlgorithm heavyHitter) {
		this.collectMin = true;
		this.collectMax = true;
		this.collectCountDistinct = true;
		this.collectHeavyHitters = true;
		this.countDistinctAlgorithm = countDistinct;
		this.heavyHitterAlgorithm = heavyHitter;
	}

	public enum CountDistinctAlgorithm {

		LINEAR_COUNTING,
		HYPERLOGLOG;

	}

	public enum HeavyHitterAlgorithm{
		LOSSY_COUNTING,
		COUNT_MIN_SKETCH;
	}

	public void setCountDbitmap(int countDbitmap) {
		this.countDbitmap = countDbitmap;
	}

	public void setCountDlog2m(int countDlog2m) {
		this.countDlog2m = countDlog2m;
	}

	public void setHeavyHitterConfidence(double heavyHitterConfidence) {
		this.heavyHitterConfidence = heavyHitterConfidence;
	}

	public void setHeavyHitterSeed(int heavyHitterSeed) {
		this.heavyHitterSeed = heavyHitterSeed;
	}

	public void setHeavyHitterFraction(double heavyHitterFraction) {
		this.heavyHitterFraction = heavyHitterFraction;
	}

	public void setHeavyHitterError(double heavyHitterError) {
		this.heavyHitterError = heavyHitterError;
	}

	public void setCollectMin(boolean collectMin) {
		this.collectMin = collectMin;
	}

	public void setCollectMax(boolean collectMax) {
		this.collectMax = collectMax;
	}

	public void setCollectCountDistinct(boolean collectCountDistinct) {
		this.collectCountDistinct = collectCountDistinct;
	}

	public void setCollectHeavyHitters(boolean collectHeavyHitters) {
		this.collectHeavyHitters = collectHeavyHitters;
	}

	public void setCountDistinctAlgorithm(CountDistinctAlgorithm countDistinctAlgorithm) {
		this.countDistinctAlgorithm = countDistinctAlgorithm;
	}

	public void setHeavyHitterAlgorithm(HeavyHitterAlgorithm heavyHitterAlgorithm) {
		this.heavyHitterAlgorithm = heavyHitterAlgorithm;
	}

	public int getCountDbitmap() {
		return countDbitmap;
	}

	public int getCountDlog2m() {
		return countDlog2m;
	}

	public int getHeavyHitterSeed() {
		return heavyHitterSeed;
	}

	public double getHeavyHitterConfidence() {
		return heavyHitterConfidence;
	}

	public double getHeavyHitterFraction() {
		return heavyHitterFraction;
	}

	public double getHeavyHitterError() {
		return heavyHitterError;
	}
}
