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

package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.Public;

/**
 * An accumulator that computes the average value.
 * Input can be {@code long}, {@code integer}, or {@code double} and the result is {@code double}.
 */
@Public
public class AverageAccumulator implements SimpleAccumulator<Double> {

	private static final long serialVersionUID = 3672555084179165255L;

	private long count;

	private double sum;

	@Override
	public void add(Double value) {
		this.count++;
		this.sum += value;
	}

	public void add(double value) {
		this.count++;
		this.sum += value;
	}

	public void add(long value) {
		this.count++;
		this.sum += value;
	}

	public void add(int value) {
		this.count++;
		this.sum += value;
	}

	@Override
	public Double getLocalValue() {
		if (this.count == 0) {
			return 0.0;
		}
		return this.sum / this.count;
	}

	@Override
	public void resetLocal() {
		this.count = 0;
		this.sum = 0;
	}

	@Override
	public void merge(Accumulator<Double, Double> other) {
		if (other instanceof AverageAccumulator) {
			AverageAccumulator avg = (AverageAccumulator)other;
			this.count += avg.count;
			this.sum += avg.sum;
		} else {
			throw new IllegalArgumentException("The merged accumulator must be AverageAccumulator.");
		}
	}

	@Override
	public AverageAccumulator clone() {
		AverageAccumulator average = new AverageAccumulator();
		average.count = this.count;
		average.sum = this.sum;
		return average;
	}

	@Override
	public String toString() {
		return "AverageAccumulator " + this.getLocalValue() + " for " + this.count + " elements";
	}
}
