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

package org.apache.flink.table.planner.calcite;

import org.apache.calcite.schema.SchemaVersion;

/** Schema version for time travel. */
public abstract class FlinkSchemaVersion implements SchemaVersion {

    public static SchemaVersion of(long timestamp) {
        return new TimestampSchemaVersion(timestamp);
    }

    /** The implementation of {@link SchemaVersion} to specify the snapshot at the specific time. */
    public static class TimestampSchemaVersion extends FlinkSchemaVersion {

        private final long timestamp;

        public TimestampSchemaVersion(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public boolean isBefore(SchemaVersion other) {
            if (!(other instanceof TimestampSchemaVersion)) {
                throw new IllegalArgumentException(
                        "Cannot compare a TimestampSchemaVersion object with a "
                                + other.getClass()
                                + " object.");
            } else {
                return this.timestamp < ((TimestampSchemaVersion) other).timestamp;
            }
        }
    }
}
