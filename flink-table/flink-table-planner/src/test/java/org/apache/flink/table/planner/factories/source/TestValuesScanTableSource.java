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

package org.apache.flink.table.planner.factories.source;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Values {@link ScanTableSource} for testing that supports projection push down. */
public class TestValuesScanTableSource extends TestValuesScanTableSourceWithoutProjectionPushDown
        implements SupportsProjectionPushDown {

    public TestValuesScanTableSource(
            DataType producedDataType,
            ChangelogMode changelogMode,
            boolean bounded,
            String runtimeSource,
            boolean failingSource,
            Map<Map<String, String>, Collection<Row>> data,
            boolean nestedProjectionSupported,
            @Nullable int[][] projectedPhysicalFields,
            List<ResolvedExpression> filterPredicates,
            Set<String> filterableFields,
            int numElementToSkip,
            long limit,
            List<Map<String, String>> allPartitions,
            Map<String, DataType> readableMetadata,
            @Nullable int[] projectedMetadataFields) {
        super(
                producedDataType,
                changelogMode,
                bounded,
                runtimeSource,
                failingSource,
                data,
                nestedProjectionSupported,
                projectedPhysicalFields,
                filterPredicates,
                filterableFields,
                numElementToSkip,
                limit,
                allPartitions,
                readableMetadata,
                projectedMetadataFields);
    }

    @Override
    public DynamicTableSource copy() {
        return new TestValuesScanTableSource(
                producedDataType,
                changelogMode,
                bounded,
                runtimeSource,
                failingSource,
                data,
                nestedProjectionSupported,
                projectedPhysicalFields,
                filterPredicates,
                filterableFields,
                numElementToSkip,
                limit,
                allPartitions,
                readableMetadata,
                projectedMetadataFields);
    }

    @Override
    public boolean supportsNestedProjection() {
        return nestedProjectionSupported;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.producedDataType = producedDataType;
        this.projectedPhysicalFields = projectedFields;
        // we can't immediately project the data here,
        // because ReadingMetadataSpec may bring new fields
    }
}
