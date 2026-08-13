/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.arrow.vectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.TimestampColumnVector;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** Arrow column vector for Timestamp. */
@Internal
public final class ArrowTimestampColumnVector implements TimestampColumnVector {

    /** Container which is used to store the sequence of timestamp values of a column to read. */
    private final TimeStampVector valueVector;

    private final TimeUnit timeUnit;

    public ArrowTimestampColumnVector(TimeStampVector valueVector) {
        this.valueVector = Preconditions.checkNotNull(valueVector);
        this.timeUnit = ((ArrowType.Timestamp) valueVector.getField().getType()).getUnit();
    }

    @Override
    public TimestampData getTimestamp(int i, int precision) {
        long value = valueVector.get(i);
        if (timeUnit == TimeUnit.SECOND) {
            return TimestampData.fromEpochMillis(value * 1000);
        } else if (timeUnit == TimeUnit.MILLISECOND) {
            return TimestampData.fromEpochMillis(value);
        } else if (timeUnit == TimeUnit.MICROSECOND) {
            return TimestampData.fromEpochMillis(
                    Math.floorDiv(value, 1000), (int) Math.floorMod(value, 1000) * 1000);
        } else {
            return TimestampData.fromEpochMillis(
                    Math.floorDiv(value, 1_000_000), (int) Math.floorMod(value, 1_000_000));
        }
    }

    @Override
    public boolean isNullAt(int i) {
        return valueVector.isNull(i);
    }
}
