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

import org.apache.flink.annotation.PublicEvolving;

/**
 * An accumulator that sums up {@code long} values.
 */
@PublicEvolving
public class LongCounter implements SimpleAccumulator<Long> {

	private static final long serialVersionUID = 1L;

	private long localValue;

	public LongCounter() {}

	public LongCounter(long value) {
		this.localValue = value;
	}

	// ------------------------------------------------------------------------
	//  Accumulator
	// ------------------------------------------------------------------------

	/**
	 * Consider using {@link #add(long)} instead for primitive long values
	 */
	@Override
	public void add(Long value) {
		this.localValue += value;
	}

	@Override
	public Long getLocalValue() {
		return this.localValue;
	}

	@Override
	public void merge(Accumulator<Long, Long> other) {
		this.localValue += other.getLocalValue();
	}

	@Override
	public void resetLocal() {
		this.localValue = 0;
	}

	@Override
	public LongCounter clone() {
		LongCounter result = new LongCounter();
		result.localValue = localValue;
		return result;
	}

	// ------------------------------------------------------------------------
	//  Primitive Specializations
	// ------------------------------------------------------------------------

	public void add(long value){
		this.localValue += value;
	}

	public long getLocalValuePrimitive() {
		return this.localValue;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "LongCounter " + this.localValue;
	}
}
