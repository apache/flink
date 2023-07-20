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

package org.apache.flink.table.sources.tsextractors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedFieldReference;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.STREAM_RECORD_TIMESTAMP;

/**
 * Extracts the timestamp of a StreamRecord into a rowtime attribute.
 *
 * <p>Note: This extractor only works for StreamTableSources.
 *
 * @deprecated This class will not be supported in the new source design around {@link
 *     org.apache.flink.table.connector.source.DynamicTableSource}. See FLIP-95 for more
 *     information.
 */
@Deprecated
@PublicEvolving
public final class StreamRecordTimestamp extends TimestampExtractor {

    private static final long serialVersionUID = 1L;

    public static final StreamRecordTimestamp INSTANCE = new StreamRecordTimestamp();

    @Override
    public String[] getArgumentFields() {
        return new String[0];
    }

    @Override
    public void validateArgumentFields(TypeInformation<?>[] argumentFieldTypes) {}

    @Override
    public Expression getExpression(ResolvedFieldReference[] fieldAccesses) {
        return ApiExpressionUtils.unresolvedCall(STREAM_RECORD_TIMESTAMP);
    }

    @Override
    public Map<String, String> toProperties() {
        Map<String, String> map = new HashMap<>();
        map.put(Rowtime.ROWTIME_TIMESTAMPS_TYPE, Rowtime.ROWTIME_TIMESTAMPS_TYPE_VALUE_FROM_SOURCE);
        return map;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode();
    }
}
