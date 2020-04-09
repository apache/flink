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
import org.apache.flink.table.dataformat.SqlTimestamp;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * {@link ArrowFieldWriter} for Timestamp.
 */
@Internal
public final class TimestampWriter<T extends TypeGetterSetters> extends ArrowFieldWriter<T> {

	private final int precision;

	public TimestampWriter(ValueVector valueVector, int precision) {
		super(valueVector);
		Preconditions.checkState(valueVector instanceof TimeStampVector && ((ArrowType.Timestamp) valueVector.getField().getType()).getTimezone() == null);
		this.precision = precision;
	}

	@Override
	public void doWrite(T row, int ordinal) {
		ValueVector valueVector = getValueVector();
		if (row.isNullAt(ordinal)) {
			((TimeStampVector) valueVector).setNull(getCount());
		} else {
			SqlTimestamp sqlTimestamp = row.getTimestamp(ordinal, precision);

			if (valueVector instanceof TimeStampSecVector) {
				((TimeStampSecVector) valueVector).setSafe(getCount(), sqlTimestamp.getMillisecond() / 1000);
			} else if (valueVector instanceof TimeStampMilliVector) {
				((TimeStampMilliVector) valueVector).setSafe(getCount(), sqlTimestamp.getMillisecond());
			} else if (valueVector instanceof TimeStampMicroVector) {
				((TimeStampMicroVector) valueVector).setSafe(getCount(), sqlTimestamp.getMillisecond() * 1000 + sqlTimestamp.getNanoOfMillisecond() / 1000);
			} else {
				((TimeStampNanoVector) valueVector).setSafe(getCount(), sqlTimestamp.getMillisecond() * 1_000_000 + sqlTimestamp.getNanoOfMillisecond());
			}
		}
	}
}
