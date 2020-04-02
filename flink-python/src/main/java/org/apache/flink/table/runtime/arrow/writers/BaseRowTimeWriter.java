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
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.ValueVector;

/**
 * {@link ArrowFieldWriter} for Time.
 */
@Internal
public final class BaseRowTimeWriter extends ArrowFieldWriter<BaseRow> {

	public BaseRowTimeWriter(ValueVector valueVector) {
		super(valueVector);
		Preconditions.checkState(
			valueVector instanceof TimeSecVector ||
				valueVector instanceof TimeMilliVector ||
				valueVector instanceof TimeMicroVector ||
				valueVector instanceof TimeNanoVector);
	}

	@Override
	public void doWrite(BaseRow row, int ordinal) {
		ValueVector valueVector = getValueVector();
		if (row.isNullAt(ordinal)) {
			((BaseFixedWidthVector) valueVector).setNull(getCount());
		} else if (valueVector instanceof TimeSecVector) {
			((TimeSecVector) valueVector).setSafe(getCount(), row.getInt(ordinal) / 1000);
		} else if (valueVector instanceof TimeMilliVector) {
			((TimeMilliVector) valueVector).setSafe(getCount(), row.getInt(ordinal));
		} else if (valueVector instanceof TimeMicroVector) {
			((TimeMicroVector) valueVector).setSafe(getCount(), row.getInt(ordinal) * 1000L);
		} else {
			((TimeNanoVector) valueVector).setSafe(getCount(), row.getInt(ordinal) * 1000000L);
		}
	}
}
