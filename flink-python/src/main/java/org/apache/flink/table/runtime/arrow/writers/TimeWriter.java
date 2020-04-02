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

package org.apache.flink.table.runtime.arrow.writers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.ValueVector;

import java.sql.Time;
import java.util.TimeZone;

/**
 * {@link ArrowFieldWriter} for Time.
 */
@Internal
public final class TimeWriter extends ArrowFieldWriter<Row> {

	// The local time zone.
	private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

	private static final long MILLIS_PER_DAY = 86400000L; // = 24 * 60 * 60 * 1000

	public TimeWriter(ValueVector valueVector) {
		super(valueVector);
		Preconditions.checkState(
			valueVector instanceof TimeSecVector ||
				valueVector instanceof TimeMilliVector ||
				valueVector instanceof TimeMicroVector ||
				valueVector instanceof TimeNanoVector);
	}

	@Override
	public void doWrite(Row row, int ordinal) {
		ValueVector valueVector = getValueVector();
		if (row.getField(ordinal) == null) {
			((BaseFixedWidthVector) getValueVector()).setNull(getCount());
		} else {
			Time time = (Time) row.getField(ordinal);
			long ts = time.getTime() + LOCAL_TZ.getOffset(time.getTime());
			int timeMilli = (int) (ts % MILLIS_PER_DAY);

			if (valueVector instanceof TimeSecVector) {
				((TimeSecVector) valueVector).setSafe(getCount(), timeMilli / 1000);
			} else if (valueVector instanceof TimeMilliVector) {
				((TimeMilliVector) valueVector).setSafe(getCount(), timeMilli);
			} else if (valueVector instanceof TimeMicroVector) {
				((TimeMicroVector) valueVector).setSafe(getCount(), timeMilli * 1000L);
			} else {
				((TimeNanoVector) valueVector).setSafe(getCount(), timeMilli * 1000000L);
			}
		}
	}
}
