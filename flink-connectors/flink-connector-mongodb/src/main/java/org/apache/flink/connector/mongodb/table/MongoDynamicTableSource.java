/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.mongodb.common.config.MongoConnectionOptions;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.config.MongoReadOptions;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.connector.mongodb.table.serialization.MongoRowDataDeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** A {@link DynamicTableSource} for MongoDB. */
@Internal
public class MongoDynamicTableSource
        implements ScanTableSource,
                LookupTableSource,
                SupportsProjectionPushDown,
                SupportsLimitPushDown {

    private final MongoConnectionOptions connectionOptions;
    private final MongoReadOptions readOptions;
    @Nullable private final LookupCache lookupCache;
    private final int lookupRetryTimes;
    private DataType physicalRowDataType;
    private int limit = -1;

    public MongoDynamicTableSource(
            MongoConnectionOptions connectionOptions,
            MongoReadOptions readOptions,
            @Nullable LookupCache lookupCache,
            int lookupRetryTimes,
            DataType physicalRowDataType) {
        this.connectionOptions = connectionOptions;
        this.readOptions = readOptions;
        this.lookupCache = lookupCache;
        this.lookupRetryTimes = lookupRetryTimes;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        final List<String> keyNames = new ArrayList<>(context.getKeys().length);
        for (int i = 0; i < context.getKeys().length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "MongoDB only support non-nested look up keys yet");
            keyNames.add(DataType.getFieldNames(physicalRowDataType).get(innerKeyArr[0]));
        }
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();

        MongoRowDataLookupFunction lookupFunction =
                new MongoRowDataLookupFunction(
                        connectionOptions,
                        lookupRetryTimes,
                        DataType.getFieldNames(physicalRowDataType),
                        DataType.getFieldDataTypes(physicalRowDataType),
                        keyNames,
                        rowType);
        if (lookupCache != null) {
            return PartialCachingLookupProvider.of(lookupFunction, lookupCache);
        } else {
            return LookupFunctionProvider.of(lookupFunction);
        }
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        final TypeInformation<RowData> typeInfo =
                runtimeProviderContext.createTypeInformation(physicalRowDataType);

        final MongoDeserializationSchema<RowData> deserializationSchema =
                new MongoRowDataDeserializationSchema(rowType, typeInfo);

        MongoSource<RowData> mongoSource =
                MongoSource.<RowData>builder()
                        .setUri(connectionOptions.getUri())
                        .setDatabase(connectionOptions.getDatabase())
                        .setCollection(connectionOptions.getCollection())
                        .setFetchSize(readOptions.getFetchSize())
                        .setCursorBatchSize(readOptions.getCursorBatchSize())
                        .setNoCursorTimeout(readOptions.isNoCursorTimeout())
                        .setPartitionStrategy(readOptions.getPartitionStrategy())
                        .setPartitionSize(readOptions.getPartitionSize())
                        .setSamplesPerPartition(readOptions.getSamplesPerPartition())
                        .setLimit(limit)
                        .setProjectedFields(DataType.getFieldNames(physicalRowDataType))
                        .setDeserializationSchema(deserializationSchema)
                        .build();

        return SourceProvider.of(mongoSource);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public DynamicTableSource copy() {
        return new MongoDynamicTableSource(
                connectionOptions, readOptions, lookupCache, lookupRetryTimes, physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB";
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = (int) limit;
    }

    @Override
    public boolean supportsNestedProjection() {
        // planner doesn't support nested projection push down yet.
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.physicalRowDataType = Projection.of(projectedFields).project(physicalRowDataType);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MongoDynamicTableSource)) {
            return false;
        }
        MongoDynamicTableSource that = (MongoDynamicTableSource) o;
        return Objects.equals(connectionOptions, that.connectionOptions)
                && Objects.equals(readOptions, that.readOptions)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(limit, that.limit)
                && Objects.equals(lookupCache, that.lookupCache)
                && Objects.equals(lookupRetryTimes, that.lookupRetryTimes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                connectionOptions,
                readOptions,
                physicalRowDataType,
                limit,
                lookupCache,
                lookupRetryTimes);
    }
}
