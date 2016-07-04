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

package org.apache.flink.util;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Value object that represents time interval
 * */
@PublicEvolving
public final class TimeInterval implements Serializable {
	private final long length;
	private final TimeUnit unit;

	public TimeInterval(long length, TimeUnit unit) {
		this.length = length;
		this.unit = unit;
	}

	public long getLength() {
		return length;
	}

	public TimeUnit getUnit() {
		return unit;
	}

	@Override
	public String toString() {
		return "TimeInterval{" +
				"length=" + length +
				", unit=" + unit +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TimeInterval) {
			TimeInterval thatInMillis = ((TimeInterval) o).toUnit(TimeUnit.MILLISECONDS);
			return this.toUnit(TimeUnit.MILLISECONDS).getLength() == thatInMillis.getLength();
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int result = (int) (length ^ (length >>> 32));
		result = 31 * result + (unit != null ? unit.hashCode() : 0);
		return result;
	}

	public long getMillis() {
		return toUnit(TimeUnit.MILLISECONDS).getLength();
	}

	private TimeInterval toUnit(TimeUnit targetUnit) {
		long convertedLength = targetUnit.convert(length, unit);
		return new TimeInterval(convertedLength, targetUnit);
	}
}