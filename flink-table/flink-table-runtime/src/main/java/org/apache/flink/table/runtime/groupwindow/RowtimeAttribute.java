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

package org.apache.flink.table.runtime.groupwindow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Rowtime property.
 *
 * @deprecated The POJOs in this package are used to represent the deprecated Group Window feature.
 *     Currently, they also used to configure Python operators.
 */
@Deprecated
@Internal
@JsonTypeName("Rowtime")
public class RowtimeAttribute extends AbstractWindowProperty {

    @JsonCreator
    public RowtimeAttribute(@JsonProperty(FIELD_NAME_REFERENCE) WindowReference reference) {
        super(reference);
    }

    @Override
    public LogicalType getResultType() {
        if (reference.getType().isPresent()) {
            LogicalType resultType = reference.getType().get();
            if (resultType instanceof TimestampType
                    && ((TimestampType) resultType).getKind() == TimestampKind.ROWTIME) {
                // rowtime window with TIMESTAMP type
                return new TimestampType(true, TimestampKind.ROWTIME, 3);
            } else if (resultType instanceof LocalZonedTimestampType
                    && ((LocalZonedTimestampType) resultType).getKind() == TimestampKind.ROWTIME) {
                // rowtime window with TIMESTAMP_LTZ type
                return new LocalZonedTimestampType(true, TimestampKind.ROWTIME, 3);
            } else if (resultType instanceof BigIntType || resultType instanceof TimestampType) {
                // batch time window
                return new TimestampType(3);
            }
        }
        throw new TableException(
                "WindowReference of RowtimeAttribute has invalid type. Please report this bug.");
    }

    @Override
    public String toString() {
        return String.format("rowtime(%s)", reference);
    }
}
