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

/**
 * An accumulator that finds the maximum {@code double} value.
 */
public class DoubleMaximum implements SimpleAccumulator<Double> {

	private static final long serialVersionUID = 1L;

	private double max = Double.NEGATIVE_INFINITY;

	/**
	 * Consider using {@link #add(double)} instead for primitive double values
	 */
	@Override
	public void add(Double value) {
		this.max = Math.max(this.max, value);
	}

	public void add(double value) {
		this.max = Math.max(this.max, value);
	}

	@Override
	public Double getLocalValue() {
		return this.max;
	}

	@Override
	public void resetLocal() {
		this.max = Double.NEGATIVE_INFINITY;
	}

	@Override
	public void merge(Accumulator<Double, Double> other) {
		this.max = Math.max(this.max, other.getLocalValue());
	}

	@Override
	public DoubleMaximum clone() {
		DoubleMaximum clone = new DoubleMaximum();
		clone.max = this.max;
		return clone;
	}

	@Override
	public String toString() {
		return "DoubleMaximum " + this.max;
	}
}
