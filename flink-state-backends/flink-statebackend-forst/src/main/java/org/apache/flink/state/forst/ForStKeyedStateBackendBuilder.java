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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateBackendBuilder;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.state.forst.restore.ForStNoneRestoreOperation;
import org.apache.flink.state.forst.restore.ForStRestoreOperation;
import org.apache.flink.state.forst.restore.ForStRestoreResult;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Function;

/**
 * Builder class for {@link ForStKeyedStateBackend} which handles all necessary initializations and
 * cleanups.
 *
 * @param <K> The data type that the key serializer serializes.
 */
public class ForStKeyedStateBackendBuilder<K>
        implements StateBackendBuilder<ForStKeyedStateBackend<K>, BackendBuildingException> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final StateSerializerProvider<K> keySerializerProvider;
    private final Collection<KeyedStateHandle> restoreStateHandles;

    /** Factory function to create column family options from state name. */
    private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;

    /** The container of ForSt option factory and predefined options. */
    private final ForStResourceContainer optionsContainer;

    /** Path where this configured instance stores its data directory. */
    private final File localBasePath;

    /** Path where this configured instance stores its ForSt database. */
    private final File localForStPath;

    private final MetricGroup metricGroup;

    /** ForSt property-based and statistics-based native metrics options. */
    private ForStNativeMetricOptions nativeMetricOptions;

    public ForStKeyedStateBackendBuilder(
            ForStResourceContainer optionsContainer,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            TypeSerializer<K> keySerializer,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles) {
        this.optionsContainer = optionsContainer;
        this.localBasePath = optionsContainer.getLocalBasePath();
        this.localForStPath = optionsContainer.getLocalForStPath();
        this.columnFamilyOptionsFactory = Preconditions.checkNotNull(columnFamilyOptionsFactory);
        this.keySerializerProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(keySerializer);
        this.metricGroup = metricGroup;
        this.restoreStateHandles = stateHandles;
        this.nativeMetricOptions = new ForStNativeMetricOptions();
    }

    ForStKeyedStateBackendBuilder<K> setNativeMetricOptions(
            ForStNativeMetricOptions nativeMetricOptions) {
        this.nativeMetricOptions = nativeMetricOptions;
        return this;
    }

    @Override
    public ForStKeyedStateBackend<K> build() throws BackendBuildingException {
        ColumnFamilyHandle defaultColumnFamilyHandle = null;
        ForStNativeMetricMonitor nativeMetricMonitor = null;
        RocksDB db = null;
        ForStRestoreOperation restoreOperation = null;

        try {
            prepareLocalDirectories();
            restoreOperation = getForStRestoreOperation();
            ForStRestoreResult restoreResult = restoreOperation.restore();
            db = restoreResult.getDb();
            defaultColumnFamilyHandle = restoreResult.getDefaultColumnFamilyHandle();
            nativeMetricMonitor = restoreResult.getNativeMetricMonitor();

            // TODO: Support to init snapshot strategy

        } catch (Throwable e) {
            // Do clean up
            IOUtils.closeQuietly(defaultColumnFamilyHandle);
            IOUtils.closeQuietly(nativeMetricMonitor);
            IOUtils.closeQuietly(db);
            // it's possible that db has been initialized but later restore steps failed
            IOUtils.closeQuietly(restoreOperation);
            IOUtils.closeQuietly(optionsContainer);
            try {
                FileUtils.deleteDirectory(localBasePath);
            } catch (Exception ex) {
                logger.warn("Failed to delete local base path for ForSt: " + localBasePath, ex);
            }
            // Log and rethrow
            if (e instanceof BackendBuildingException) {
                throw (BackendBuildingException) e;
            } else {
                String errMsg = "Caught unexpected exception.";
                logger.error(errMsg, e);
                throw new BackendBuildingException(errMsg, e);
            }
        }
        logger.info(
                "Finished building ForSt keyed state-backend at local base path: {}.",
                localBasePath);
        return new ForStKeyedStateBackend<>(
                this.localBasePath,
                this.optionsContainer,
                this.keySerializerProvider.currentSchemaSerializer(),
                db,
                defaultColumnFamilyHandle,
                nativeMetricMonitor);
    }

    private ForStRestoreOperation getForStRestoreOperation() {
        DBOptions dbOptions = optionsContainer.getDbOptions();
        if (CollectionUtil.isEmptyOrAllElementsNull(restoreStateHandles)) {
            return new ForStNoneRestoreOperation(
                    localForStPath,
                    dbOptions,
                    columnFamilyOptionsFactory,
                    nativeMetricOptions,
                    metricGroup);
        }
        // TODO: Support Restoring
        throw new UnsupportedOperationException("Not support restoring yet for ForStStateBackend");
    }

    private void prepareLocalDirectories() throws IOException {
        checkAndCreateDirectory(localBasePath);
        if (localForStPath.exists()) {
            // Clear the base directory when the backend is created
            // in case something crashed and the backend never reached dispose()
            FileUtils.deleteDirectory(localBasePath);
        }
    }

    private static void checkAndCreateDirectory(File directory) throws IOException {
        if (directory.exists()) {
            if (!directory.isDirectory()) {
                throw new IOException("Not a directory: " + directory);
            }
        } else if (!directory.mkdirs()) {
            throw new IOException(
                    String.format("Could not create ForSt data directory at %s.", directory));
        }
    }
}
