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
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;

import java.sql.Timestamp;

/** Converter for {@link LocalZonedTimestampType} of {@link java.sql.Timestamp} external type. */
@Internal
public class LocalZonedTimestampTimestampConverter
        implements DataStructureConverter<TimestampData, Timestamp> {

    private static final long serialVersionUID = 1L;
    private static final int NANOS_PER_MILL = 1000_000;

    @Override
    public TimestampData toInternal(Timestamp external) {
        return TimestampData.fromEpochMillis(
                external.getTime(), external.getNanos() % NANOS_PER_MILL);
    }

    @Override
    public Timestamp toExternal(TimestampData internal) {
        Timestamp ts = new Timestamp(internal.getMillisecond());
        ts.setNanos(ts.getNanos() + internal.getNanoOfMillisecond());
        return ts;
    }
}
