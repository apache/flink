/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.arrow.vectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.vector.TimestampColumnVector;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Arrow column vector for Timestamp.
 */
@Internal
public final class ArrowTimestampColumnVector implements TimestampColumnVector {

	/**
	 * Container which is used to store the sequence of timestamp values of a column to read.
	 */
	private final ValueVector valueVector;

	public ArrowTimestampColumnVector(ValueVector valueVector) {
		this.valueVector = Preconditions.checkNotNull(valueVector);
		Preconditions.checkState(valueVector instanceof TimeStampVector && ((ArrowType.Timestamp) valueVector.getField().getType()).getTimezone() == null);
	}

	@Override
	public TimestampData getTimestamp(int i, int precision) {
		if (valueVector instanceof TimeStampSecVector) {
			return TimestampData.fromEpochMillis(((TimeStampSecVector) valueVector).get(i) * 1000);
		} else if (valueVector instanceof TimeStampMilliVector) {
			return TimestampData.fromEpochMillis(((TimeStampMilliVector) valueVector).get(i));
		} else if (valueVector instanceof TimeStampMicroVector) {
			long micros = ((TimeStampMicroVector) valueVector).get(i);
			return TimestampData.fromEpochMillis(micros / 1000, (int) (micros % 1000) * 1000);
		} else {
			long nanos = ((TimeStampNanoVector) valueVector).get(i);
			return TimestampData.fromEpochMillis(nanos / 1_000_000, (int) (nanos % 1_000_000));
		}
	}

	@Override
	public boolean isNullAt(int i) {
		return valueVector.isNull(i);
	}
}
