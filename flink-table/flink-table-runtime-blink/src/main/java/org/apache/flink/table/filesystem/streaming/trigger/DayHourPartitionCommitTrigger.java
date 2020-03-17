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

package org.apache.flink.table.filesystem.streaming.trigger;

import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;

import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * Day-hour trigger: day-hour: two partition columns. The first partition field like
 * {@link DayPartitionCommitTrigger}, represents day. The second format is ‘HH’ represents hour.
 */
public class DayHourPartitionCommitTrigger extends DayPartitionCommitTrigger {

	@Override
	public boolean canCommit(LinkedHashMap<String, String> partitionSpec, long watermark) {
		Iterator<String> iter = partitionSpec.values().iterator();
		String day = iter.hasNext() ? iter.next() : null;
		String hour = iter.hasNext() ? iter.next() : null;
		return watermark >= dayToMillisecond(day) +
				hourToMillisecond(hour) +
				SqlDateTimeUtils.MILLIS_PER_HOUR;
	}

	private long hourToMillisecond(String hourField) {
		int hour = Integer.parseInt(hourField);
		if (hour > 23) {
			throw new IllegalArgumentException(
					String.format("Hour %s can not bigger than 23.", hourField));
		}
		return hour * SqlDateTimeUtils.MILLIS_PER_HOUR;
	}
}
