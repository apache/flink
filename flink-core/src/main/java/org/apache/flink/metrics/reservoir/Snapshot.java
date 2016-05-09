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
package org.apache.flink.metrics.reservoir;

import org.apache.flink.annotation.PublicEvolving;

@PublicEvolving
public interface Snapshot {
	/**
	 * Returns the value at the given quantile.
	 *
	 * @param quantile a given quantile, in {@code [0..1]}
	 * @return the value in the distribution at {@code quantile}
	 */
	double getValue(double quantile);

	/**
	 * Returns the entire set of values in the snapshot.
	 *
	 * @return the entire set of values
	 */
	long[] getValues();

	/**
	 * Returns the number of values in the snapshot.
	 *
	 * @return the number of values
	 */
	int size();

	/**
	 * Returns the highest value in the snapshot.
	 *
	 * @return the highest value
	 */
	long getMax();

	/**
	 * Returns the arithmetic mean of the values in the snapshot.
	 *
	 * @return the arithmetic mean
	 */
	double getMean();

	/**
	 * Returns the lowest value in the snapshot.
	 *
	 * @return the lowest value
	 */
	long getMin();

	/**
	 * Returns the standard deviation of the values in the snapshot.
	 *
	 * @return the standard value
	 */
	double getStdDev();

	/**
	 * Returns the median value in the distribution.
	 *
	 * @return the median value
	 */
	double getMedian();

	/**
	 * Returns the value at the 75th percentile in the distribution.
	 *
	 * @return the value at the 75th percentile
	 */
	double get75thPercentile();

	/**
	 * Returns the value at the 95th percentile in the distribution.
	 *
	 * @return the value at the 95th percentile
	 */
	double get95thPercentile();

	/**
	 * Returns the value at the 98th percentile in the distribution.
	 *
	 * @return the value at the 98th percentile
	 */
	double get98thPercentile();

	/**
	 * Returns the value at the 99th percentile in the distribution.
	 *
	 * @return the value at the 99th percentile
	 */
	double get99thPercentile();

	/**
	 * Returns the value at the 99.9th percentile in the distribution.
	 *
	 * @return the value at the 99.9th percentile
	 */
	double get999thPercentile();
}
