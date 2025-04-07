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

package org.apache.flink.state.forst.sync;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.core.fs.ICloseableRegistry;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.KeyGroupPartitionedPriorityQueue;
import org.apache.flink.state.forst.ForStDBWriteBatchWrapper;
import org.apache.flink.state.forst.ForStNativeMetricMonitor;
import org.apache.flink.state.forst.ForStOperationUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.ReadOptions;
import org.forstdb.RocksDB;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.function.Function;

/**
 * Encapsulates the logic and resources in connection with creating priority queue state structures,
 * for RocksDB backend.
 */
public class ForStDBPriorityQueueSetFactory implements PriorityQueueSetFactory {

    /** The priorityQueue cache size per key-group. */
    private final int cacheSize;

    /** A shared buffer to serialize elements for the priority queue. */
    @Nonnull private final DataOutputSerializer sharedElementOutView;

    /** A shared buffer to de-serialize elements for the priority queue. */
    @Nonnull private final DataInputDeserializer sharedElementInView;

    private final KeyGroupRange keyGroupRange;
    private final int keyGroupPrefixBytes;
    private final int numberOfKeyGroups;
    private final Map<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation;
    private final RocksDB db;
    private final ReadOptions readOptions;
    private final ForStDBWriteBatchWrapper writeBatchWrapper;
    private final ForStNativeMetricMonitor nativeMetricMonitor;
    private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;
    private final Long writeBufferManagerCapacity;

    ForStDBPriorityQueueSetFactory(
            KeyGroupRange keyGroupRange,
            int keyGroupPrefixBytes,
            int numberOfKeyGroups,
            Map<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation,
            RocksDB db,
            ReadOptions readOptions,
            ForStDBWriteBatchWrapper writeBatchWrapper,
            ForStNativeMetricMonitor nativeMetricMonitor,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            Long writeBufferManagerCapacity,
            int cacheSize) {
        this.keyGroupRange = keyGroupRange;
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.numberOfKeyGroups = numberOfKeyGroups;
        this.kvStateInformation = kvStateInformation;
        this.db = db;
        this.readOptions = readOptions;
        this.writeBatchWrapper = writeBatchWrapper;
        this.nativeMetricMonitor = nativeMetricMonitor;
        this.columnFamilyOptionsFactory = columnFamilyOptionsFactory;
        this.sharedElementOutView = new DataOutputSerializer(128);
        this.sharedElementInView = new DataInputDeserializer();
        this.writeBufferManagerCapacity = writeBufferManagerCapacity;
        Preconditions.checkArgument(cacheSize > 0);
        this.cacheSize = cacheSize;
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        return create(stateName, byteOrderedElementSerializer, false);
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
                    boolean allowFutureMetadataUpdates) {

        final ForStOperationUtils.ForStKvStateInfo stateCFHandle =
                tryRegisterPriorityQueueMetaInfo(
                        stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);

        final ColumnFamilyHandle columnFamilyHandle = stateCFHandle.columnFamilyHandle;

        return new KeyGroupPartitionedPriorityQueue<>(
                KeyExtractorFunction.forKeyedObjects(),
                PriorityComparator.forPriorityComparableObjects(),
                new KeyGroupPartitionedPriorityQueue.PartitionQueueSetFactory<
                        T, ForStDBCachingPriorityQueueSet<T>>() {
                    @Nonnull
                    @Override
                    public ForStDBCachingPriorityQueueSet<T> create(
                            int keyGroupId,
                            int numKeyGroups,
                            @Nonnull KeyExtractorFunction<T> keyExtractor,
                            @Nonnull PriorityComparator<T> elementPriorityComparator) {
                        TreeOrderedSetCache orderedSetCache = new TreeOrderedSetCache(cacheSize);
                        return new ForStDBCachingPriorityQueueSet<>(
                                keyGroupId,
                                keyGroupPrefixBytes,
                                db,
                                readOptions,
                                columnFamilyHandle,
                                byteOrderedElementSerializer,
                                sharedElementOutView,
                                sharedElementInView,
                                writeBatchWrapper,
                                orderedSetCache);
                    }
                },
                keyGroupRange,
                numberOfKeyGroups);
    }

    @Nonnull
    private <T> ForStOperationUtils.ForStKvStateInfo tryRegisterPriorityQueueMetaInfo(
            @Nonnull String stateName,
            @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
            boolean allowFutureMetadataUpdates) {

        ForStOperationUtils.ForStKvStateInfo stateInfo = kvStateInformation.get(stateName);

        if (stateInfo == null) {
            // Currently this class is for timer service and TTL feature is not applicable here,
            // so no need to register compact filter when creating column family
            RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo =
                    new RegisteredPriorityQueueStateBackendMetaInfo<>(
                            stateName, byteOrderedElementSerializer);

            metaInfo =
                    allowFutureMetadataUpdates
                            ? metaInfo.withSerializerUpgradesAllowed()
                            : metaInfo;

            stateInfo =
                    ForStOperationUtils.createStateInfo(
                            metaInfo,
                            db,
                            columnFamilyOptionsFactory,
                            null,
                            writeBufferManagerCapacity,
                            // Using ICloseableRegistry.NO_OP here because there is no restore in
                            // progress; created column families will be closed in dispose()
                            ICloseableRegistry.NO_OP);
            ForStOperationUtils.registerKvStateInformation(
                    kvStateInformation, nativeMetricMonitor, stateName, stateInfo);
        } else {
            // TODO we implement the simple way of supporting the current functionality, mimicking
            // keyed state
            // because this should be reworked in FLINK-9376 and then we should have a common
            // algorithm over
            // StateMetaInfoSnapshot that avoids this code duplication.

            @SuppressWarnings("unchecked")
            RegisteredPriorityQueueStateBackendMetaInfo<T> castedMetaInfo =
                    (RegisteredPriorityQueueStateBackendMetaInfo<T>) stateInfo.metaInfo;

            TypeSerializer<T> previousElementSerializer =
                    castedMetaInfo.getPreviousElementSerializer();

            if (previousElementSerializer != byteOrderedElementSerializer) {
                TypeSerializerSchemaCompatibility<T> compatibilityResult =
                        castedMetaInfo.updateElementSerializer(byteOrderedElementSerializer);

                // Since priority queue elements are written into RocksDB
                // as keys prefixed with the key group and namespace, we do not support
                // migrating them. Therefore, here we only check for incompatibility.
                if (compatibilityResult.isIncompatible()) {
                    throw new FlinkRuntimeException(
                            new StateMigrationException(
                                    "The new priority queue serializer must not be incompatible."));
                }

                RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo =
                        new RegisteredPriorityQueueStateBackendMetaInfo<>(
                                stateName, byteOrderedElementSerializer);

                metaInfo =
                        allowFutureMetadataUpdates
                                ? metaInfo.withSerializerUpgradesAllowed()
                                : metaInfo;

                // update meta info with new serializer
                stateInfo =
                        new ForStOperationUtils.ForStKvStateInfo(
                                stateInfo.columnFamilyHandle, metaInfo);
                kvStateInformation.put(stateName, stateInfo);
            }
        }

        return stateInfo;
    }

    @VisibleForTesting
    public int getCacheSize() {
        return cacheSize;
    }
}
