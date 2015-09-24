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

package org.apache.flink.streaming.api.windowing.windowpolicy;


import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;

/**
 * A windowing policy that generates windows based on a delta between elements.
 */
public final class Delta<T> extends WindowPolicy {

	private static final long serialVersionUID = 3197290738634320211L;

	private DeltaFunction<T> deltaFunction;

	private double threshold;

	/** Instantiation only via factory method */
	private Delta(DeltaFunction<T> deltaFunction, double threshold) {
		this.deltaFunction = deltaFunction;
		this.threshold = threshold;
	}

	public DeltaFunction<T> getDeltaFunction() {
		return deltaFunction;
	}

	public double getThreshold() {
		return threshold;
	}

	@Override
	public String toString() {
		return "Delta Window (" + deltaFunction + ", " + threshold + ')';
	}

	// ------------------------------------------------------------------------
	//  Factory
	// ------------------------------------------------------------------------

	/**
	 * Creates a delta based windowing policy
	 *
	 * @param threshold The threshold for deltas at which to trigger windows
	 * @param deltaFunction The delta function
	 * @return The time policy.
	 */
	public static <T> Delta<T> of(double threshold, DeltaFunction<T> deltaFunction) {
		return new Delta<T>(deltaFunction, threshold);
	}
}
