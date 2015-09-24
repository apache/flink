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


/**
 * A windowing policy that generates windows based on element counts.
 */
public final class Count extends WindowPolicy {

	private static final long serialVersionUID = 3197290738634320211L;

	private long size;

	/** Instantiation only via factory method */
	private Count(long size) {
		this.size = size;
	}

	public long getSize() {
		return size;
	}

	@Override
	public String toString() {
		return "Count Window (" + size + ')';
	}

	// ------------------------------------------------------------------------
	//  Factory
	// ------------------------------------------------------------------------

	/**
	 * Creates a count based windowing policy
	 *
	 * @param size The size of the generated windows.
	 * @return The time policy.
	 */
	public static Count of(long size) {
		return new Count(size);
	}
}
