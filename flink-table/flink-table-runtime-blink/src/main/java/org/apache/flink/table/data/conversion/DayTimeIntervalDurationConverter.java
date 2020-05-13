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

package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.DayTimeIntervalType;

import java.time.Duration;

/**
 * Converter for {@link DayTimeIntervalType} of {@link java.time.Duration} external type.
 */
@Internal
class DayTimeIntervalDurationConverter implements DataStructureConverter<Long, java.time.Duration> {

	private static final long serialVersionUID = 1L;

	@Override
	public Long toInternal(java.time.Duration external) {
		return external.toMillis();
	}

	@Override
	public java.time.Duration toExternal(Long internal) {
		return Duration.ofMillis(internal);
	}
}
