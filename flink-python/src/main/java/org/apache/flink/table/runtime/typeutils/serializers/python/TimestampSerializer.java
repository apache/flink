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

package org.apache.flink.table.runtime.typeutils.serializers.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Uses similar serialization/deserialization of SqlTimestampSerializer to serialize Timestamp. It
 * deals with Daylight saving time problem and precision problem.
 */
@Internal
public class TimestampSerializer extends TypeSerializerSingleton<Timestamp> {

    private static final long serialVersionUID = 1L;

    // the number of milliseconds in a day
    private static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000

    private final int precision;

    public TimestampSerializer(int precision) {
        this.precision = precision;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public Timestamp createInstance() {
        return new Timestamp(0L);
    }

    @Override
    public Timestamp copy(Timestamp from) {
        if (from == null) {
            return null;
        }
        Timestamp t = new Timestamp(from.getTime());
        t.setNanos(from.getNanos());
        return t;
    }

    @Override
    public Timestamp copy(Timestamp from, Timestamp reuse) {
        if (from == null) {
            return null;
        }
        reuse.setTime(from.getTime());
        reuse.setNanos(from.getNanos());
        return reuse;
    }

    @Override
    public int getLength() {
        return isCompact() ? 8 : 12;
    }

    @Override
    public void serialize(Timestamp record, DataOutputView target) throws IOException {
        if (record == null) {
            throw new IllegalArgumentException("The Timestamp record must not be null.");
        }
        LocalDateTime dateTime = record.toLocalDateTime();
        long epochDay = dateTime.toLocalDate().toEpochDay();
        long nanoOfDay = dateTime.toLocalTime().toNanoOfDay();

        long millisecond = epochDay * MILLIS_PER_DAY + nanoOfDay / 1_000_000;
        int nanoOfMillisecond = (int) (nanoOfDay % 1_000_000);

        if (isCompact()) {
            assert nanoOfMillisecond == 0;
            target.writeLong(millisecond);
        } else {
            target.writeLong(millisecond);
            target.writeInt(nanoOfMillisecond);
        }
    }

    private boolean isCompact() {
        return precision <= 3;
    }

    @Override
    public Timestamp deserialize(DataInputView source) throws IOException {
        long millisecond;
        int nanoOfMillisecond = 0;
        if (isCompact()) {
            millisecond = source.readLong();
        } else {
            millisecond = source.readLong();
            nanoOfMillisecond = source.readInt();
        }
        int date = (int) (millisecond / MILLIS_PER_DAY);
        int time = (int) (millisecond % MILLIS_PER_DAY);
        if (time < 0) {
            --date;
            time += MILLIS_PER_DAY;
        }
        long nanoOfDay = time * 1_000_000L + nanoOfMillisecond;
        LocalDate localDate = LocalDate.ofEpochDay(date);
        LocalTime localTime = LocalTime.ofNanoOfDay(nanoOfDay);
        return Timestamp.valueOf(LocalDateTime.of(localDate, localTime));
    }

    @Override
    public Timestamp deserialize(Timestamp reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<Timestamp> snapshotConfiguration() {
        return new TimestampSerializerSnapshot(precision);
    }

    /** {@link TypeSerializerSnapshot} for {@link TimestampSerializer}. */
    public static final class TimestampSerializerSnapshot
            implements TypeSerializerSnapshot<Timestamp> {

        private static final int CURRENT_VERSION = 1;

        private int previousPrecision;

        public TimestampSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        TimestampSerializerSnapshot(int precision) {
            this.previousPrecision = precision;
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeInt(previousPrecision);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            this.previousPrecision = in.readInt();
        }

        @Override
        public TypeSerializer<Timestamp> restoreSerializer() {
            return new TimestampSerializer(previousPrecision);
        }

        @Override
        public TypeSerializerSchemaCompatibility<Timestamp> resolveSchemaCompatibility(
                TypeSerializer<Timestamp> newSerializer) {
            if (!(newSerializer instanceof TimestampSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            TimestampSerializer timestampSerializer = (TimestampSerializer) newSerializer;
            if (previousPrecision != timestampSerializer.precision) {
                return TypeSerializerSchemaCompatibility.incompatible();
            } else {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
        }
    }
}
