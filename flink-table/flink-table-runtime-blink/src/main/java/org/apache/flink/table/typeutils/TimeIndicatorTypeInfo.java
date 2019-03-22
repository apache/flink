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

package org.apache.flink.table.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.SqlTimestampComparator;
import org.apache.flink.api.common.typeutils.base.SqlTimestampSerializer;

import java.sql.Timestamp;

/**
 * TypeInfo for TimeIndicator.
 */
public class TimeIndicatorTypeInfo extends SqlTimeTypeInfo<Timestamp> {

	public static final TimeIndicatorTypeInfo ROWTIME_INDICATOR = new TimeIndicatorTypeInfo(true);

	public static final TimeIndicatorTypeInfo PROCTIME_INDICATOR = new TimeIndicatorTypeInfo(false);

	private boolean isEventTime;

	public TimeIndicatorTypeInfo(boolean isEventTime) {
		//noinspection unchecked
		super(Timestamp.class, SqlTimestampSerializer.INSTANCE,
			((Class) SqlTimestampComparator.class));
		this.isEventTime = isEventTime;
	}

	@SuppressWarnings("unchecked")
	@Override
	public TypeSerializer createSerializer(ExecutionConfig executionConfig) {
		return LongSerializer.INSTANCE;
	}

	public boolean isEventTime() {
		return isEventTime;
	}

	@Override
	public String toString() {
		if (isEventTime) {
			return "TimeIndicatorTypeInfo(rowtime)";
		} else {
			return "TimeIndicatorTypeInfo(proctime)";
		}
	}

}
