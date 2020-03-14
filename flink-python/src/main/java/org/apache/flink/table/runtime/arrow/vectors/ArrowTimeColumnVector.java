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
import org.apache.flink.table.dataformat.vector.IntColumnVector;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.ValueVector;

/**
 * Arrow column vector for Time.
 */
@Internal
public final class ArrowTimeColumnVector implements IntColumnVector {

	/**
	 * Container which is used to store the sequence of time values of a column to read.
	 */
	private final ValueVector valueVector;

	public ArrowTimeColumnVector(ValueVector valueVector) {
		this.valueVector = Preconditions.checkNotNull(valueVector);
		Preconditions.checkState(
			valueVector instanceof TimeSecVector ||
				valueVector instanceof TimeMilliVector ||
				valueVector instanceof TimeMicroVector ||
				valueVector instanceof TimeNanoVector);
	}

	@Override
	public int getInt(int i) {
		if (valueVector instanceof TimeSecVector) {
			return ((TimeSecVector) valueVector).get(i) * 1000;
		} else if (valueVector instanceof TimeMilliVector) {
			return ((TimeMilliVector) valueVector).get(i);
		} else if (valueVector instanceof TimeMicroVector) {
			return (int) (((TimeMicroVector) valueVector).get(i) / 1000);
		} else {
			return (int) (((TimeNanoVector) valueVector).get(i) / 1000000);
		}
	}

	@Override
	public boolean isNullAt(int i) {
		return valueVector.isNull(i);
	}
}
