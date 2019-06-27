/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.statistics.basicstatistic;

import org.apache.flink.ml.common.matrix.Vector;

/**
 * It is the base class which is used to store vector summarizer result.
 * You can get vectorSize, sum, mean, variance, standardDeviation, and so on.
 */
public abstract class BaseVectorSummary extends BaseSummary {

	public abstract int vectorSize();

	public abstract Vector sum();

	public abstract Vector mean();

	public abstract Vector variance();

	public abstract Vector standardDeviation();

	public abstract Vector min();

	public abstract Vector max();

	public abstract Vector normL1();

	public abstract Vector normL2();

	public double sum(int idx) {
		return sum().get(idx);
	}

	public double mean(int idx) {
		return mean().get(idx);
	}

	public double variance(int idx) {
		return variance().get(idx);
	}

	public double standardDeviation(int idx) {
		return standardDeviation().get(idx);
	}

	public double min(int idx) {
		return min().get(idx);
	}

	public double max(int idx) {
		return max().get(idx);
	}

	public double normL1(int idx) {
		return normL1().get(idx);
	}

	public double normL2(int idx) {
		return normL2().get(idx);
	}

}
