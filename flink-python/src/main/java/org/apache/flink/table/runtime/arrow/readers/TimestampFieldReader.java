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
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.sql.Timestamp;

/** {@link ArrowFieldReader} for Timestamp. */
@Internal
public final class TimestampFieldReader extends ArrowFieldReader<Timestamp> {

    public TimestampFieldReader(ValueVector valueVector) {
        super(valueVector);
        Preconditions.checkState(
                valueVector instanceof TimeStampVector
                        && ((ArrowType.Timestamp) valueVector.getField().getType()).getTimezone()
                                == null);
    }

    @Override
    public Timestamp read(int i) {
        ValueVector valueVector = getValueVector();
        if (valueVector.isNull(i)) {
            return null;
        } else {
            long millisecond;
            if (valueVector instanceof TimeStampSecVector) {
                millisecond = ((TimeStampSecVector) valueVector).get(i) * 1000;
            } else if (valueVector instanceof TimeStampMilliVector) {
                millisecond = ((TimeStampMilliVector) valueVector).get(i);
            } else if (valueVector instanceof TimeStampMicroVector) {
                millisecond = ((TimeStampMicroVector) valueVector).get(i) / 1000;
            } else {
                millisecond = ((TimeStampNanoVector) valueVector).get(i) / 1_000_000;
            }
            return PythonTypeUtils.internalToTimestamp(millisecond);
        }
    }
}
