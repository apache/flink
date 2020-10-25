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

package org.apache.flink.table.runtime.arrow.readers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.ValueVector;

import java.sql.Time;
import java.util.TimeZone;

/**
 * {@link ArrowFieldReader} for Time.
 */
@Internal
public final class TimeFieldReader extends ArrowFieldReader<Time> {

	// The local time zone.
	private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

	public TimeFieldReader(ValueVector valueVector) {
		super(valueVector);
		Preconditions.checkState(
			valueVector instanceof TimeSecVector ||
				valueVector instanceof TimeMilliVector ||
				valueVector instanceof TimeMicroVector ||
				valueVector instanceof TimeNanoVector);
	}

	@Override
	public Time read(int index) {
		ValueVector valueVector = getValueVector();
		if (valueVector.isNull(index)) {
			return null;
		} else {
			long timeMilli;
			if (valueVector instanceof TimeSecVector) {
				timeMilli = ((TimeSecVector) getValueVector()).get(index) * 1000;
			} else if (valueVector instanceof TimeMilliVector) {
				timeMilli = ((TimeMilliVector) getValueVector()).get(index);
			} else if (valueVector instanceof TimeMicroVector) {
				timeMilli = ((TimeMicroVector) getValueVector()).get(index) / 1000;
			} else {
				timeMilli = ((TimeNanoVector) getValueVector()).get(index) / 1000000;
			}
			return new Time(timeMilli - LOCAL_TZ.getOffset(timeMilli));
		}
	}
}
