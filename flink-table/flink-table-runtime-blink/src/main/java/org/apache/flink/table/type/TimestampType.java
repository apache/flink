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

package org.apache.flink.table.type;

/**
 * Sql timestamp type.
 */
public class TimestampType implements AtomicType {

	private static final long serialVersionUID = 1L;

	public static final TimestampType TIMESTAMP = new TimestampType(0, "TimestampType");
	public static final TimestampType INTERVAL_MILLIS =
			new TimestampType(1, "IntervalMillis");
	public static final TimestampType ROWTIME_INDICATOR =
			new TimestampType(2, "RowTimeIndicator");
	public static final TimestampType PROCTIME_INDICATOR =
			new TimestampType(3, "ProctimeTimeIndicator");

	private int id;
	private String name;

	private TimestampType(int id, String name) {
		this.id = id;
		this.name = name;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		return id == ((TimestampType) o).id;
	}

	@Override
	public int hashCode() {
		int result = getClass().hashCode();
		result = 31 * result + name.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return name;
	}
}
