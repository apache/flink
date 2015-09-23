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

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class AbstractTimePolicy extends WindowPolicy {

	private static final long serialVersionUID = 6593098375698927728L;
	
	/** the time unit for this policy's time interval */
	private final TimeUnit unit;
	
	/** the length of this policy's time interval */
	private final long num;


	protected AbstractTimePolicy(long num, TimeUnit unit) {
		this.unit = checkNotNull(unit, "time unit may not be null");
		this.num = num;
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the time unit for this policy's time interval.
	 * @return The time unit for this policy's time interval.
	 */
	public TimeUnit getUnit() {
		return unit;
	}

	/**
	 * Gets the length of this policy's time interval.
	 * @return The length of this policy's time interval.
	 */
	public long getNum() {
		return num;
	}

	/**
	 * Converts the time interval to milliseconds.
	 * @return The time interval in milliseconds.
	 */
	public long toMilliseconds() {
		return unit.toMillis(num);
	}
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString(WindowPolicy slidePolicy) {
		if (slidePolicy == null) {
			return "Tumbling Window (" + getClass().getSimpleName() + ") (" + num + ' ' + unit.name() + ')';
		}
		else if (slidePolicy.getClass() == getClass()) {
			AbstractTimePolicy timeSlide = (AbstractTimePolicy) slidePolicy;
			
			return "Sliding Window (" + getClass().getSimpleName() + ") (length="
					+ num + ' ' + unit.name() + ", slide=" + timeSlide.num + ' ' + timeSlide.unit.name() + ')';
		}
		else {
			return super.toString(slidePolicy);
		}
	}
	
	@Override
	public int hashCode() {
		return 31 * (int) (num ^ (num >>> 32)) + unit.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj.getClass() == getClass()) {
			AbstractTimePolicy that = (AbstractTimePolicy) obj;
			return this.num == that.num && this.unit.equals(that.unit);
		}
		else {
			return false;
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " (" + num + ' ' + unit.name() + ')';
	}
}
