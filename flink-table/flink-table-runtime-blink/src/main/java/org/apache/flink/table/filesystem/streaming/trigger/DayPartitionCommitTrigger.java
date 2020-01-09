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
 * Day trigger: one partition column, format is ‘yyyy-MM-dd’. Extract and parse
 * the first partition field and check whether it should be committed.
 */
public class DayPartitionCommitTrigger implements PartitionCommitTrigger {

	@Override
	public boolean canCommit(LinkedHashMap<String, String> partitionSpec, long watermark) {
		Iterator<String> iter = partitionSpec.values().iterator();
		String day = iter.hasNext() ? iter.next() : null;
		return watermark >= dayToMillisecond(day) + SqlDateTimeUtils.MILLIS_PER_DAY;
	}

	protected long dayToMillisecond(String dayField) {
		Integer date = SqlDateTimeUtils.dateStringToUnixDate(dayField);
		if (date == null) {
			throw new RuntimeException(String.format("Can not parse %s to date.", dayField));
		}
		return date * SqlDateTimeUtils.MILLIS_PER_DAY;
	}
}
