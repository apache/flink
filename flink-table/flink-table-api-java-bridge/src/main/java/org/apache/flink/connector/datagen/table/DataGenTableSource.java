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

package org.apache.flink.connector.datagen.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.datagen.table.types.RowDataGenerator;
import org.apache.flink.connector.datagen.table.types.SequenceGeneratorFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

/**
 * A {@link ScanTableSource} that emits generated row data via the FLIP-27 {@link
 * DataGeneratorSource}. Each per-field {@link GeneratorFunction} is invoked with the current
 * sequence index to assemble a {@link RowData}.
 */
@Internal
public class DataGenTableSource implements ScanTableSource, SupportsLimitPushDown {

    private final GeneratorFunction<Long, ?>[] fieldGenerators;
    private final String tableName;
    private final DataType rowDataType;
    private final long rowsPerSecond;
    private @Nullable Long numberOfRows;
    private final @Nullable Integer parallelism;

    public DataGenTableSource(
            GeneratorFunction<Long, ?>[] fieldGenerators,
            String tableName,
            DataType rowDataType,
            long rowsPerSecond,
            @Nullable Long numberOfRows,
            @Nullable Integer parallelism) {
        this.fieldGenerators = fieldGenerators;
        this.tableName = tableName;
        this.rowDataType = rowDataType;
        this.rowsPerSecond = rowsPerSecond;
        this.numberOfRows = numberOfRows;
        this.parallelism = parallelism;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        TypeInformation<RowData> typeInfo = context.createTypeInformation(rowDataType);
        return SourceProvider.of(createSource(typeInfo), parallelism);
    }

    @VisibleForTesting
    public DataGeneratorSource<RowData> createSource(TypeInformation<RowData> typeInfo) {
        return new DataGeneratorSource<>(
                buildRowGenerator(),
                computeEffectiveCount(),
                RateLimiterStrategy.perSecond(rowsPerSecond),
                typeInfo);
    }

    /**
     * Returns the per-field generator function that produces a single {@link RowData} for a given
     * sequence index. Exposed for tests that exercise the generation logic directly.
     */
    @VisibleForTesting
    public GeneratorFunction<Long, RowData> buildRowGenerator() {
        return new RowDataGenerator(fieldGenerators, DataType.getFieldNames(rowDataType), 0);
    }

    /**
     * Computes the bound passed to {@link DataGeneratorSource}, preserving the legacy "halt as soon
     * as either the row count is reached or any sequence field is exhausted" semantic. When no
     * sequence fields are present the bound is {@code numberOfRows} if configured and {@link
     * Long#MAX_VALUE} otherwise; when sequence fields are present the bound additionally cannot
     * exceed the smallest sequence-field range, which {@link SequenceGeneratorFunction} relies on
     * to keep {@code start + idx} within {@code [start, end]}.
     */
    @VisibleForTesting
    public long computeEffectiveCount() {
        long minSequenceCount = Long.MAX_VALUE;
        boolean hasSequence = false;
        for (GeneratorFunction<Long, ?> generator : fieldGenerators) {
            if (generator instanceof SequenceGeneratorFunction) {
                hasSequence = true;
                long total = ((SequenceGeneratorFunction<?>) generator).getTotalCount();
                if (total < minSequenceCount) {
                    minSequenceCount = total;
                }
            }
        }
        if (numberOfRows != null) {
            return hasSequence ? Math.min(numberOfRows, minSequenceCount) : numberOfRows;
        }
        return hasSequence ? minSequenceCount : Long.MAX_VALUE;
    }

    @VisibleForTesting
    public GeneratorFunction<Long, ?>[] getFieldGenerators() {
        return fieldGenerators;
    }

    @VisibleForTesting
    public @Nullable Integer getParallelism() {
        return parallelism;
    }

    @Override
    public DynamicTableSource copy() {
        return new DataGenTableSource(
                fieldGenerators, tableName, rowDataType, rowsPerSecond, numberOfRows, parallelism);
    }

    @Override
    public String asSummaryString() {
        return "DataGenTableSource";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public void applyLimit(long limit) {
        this.numberOfRows = limit;
    }
}
