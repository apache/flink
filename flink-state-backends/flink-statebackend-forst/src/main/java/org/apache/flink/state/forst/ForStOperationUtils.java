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

package org.apache.flink.state.forst;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/** Utils for ForSt Operations. */
public class ForStOperationUtils {

    /**
     * The name of the merge operator in ForSt. Do not change except you know exactly what you do.
     */
    public static final String MERGE_OPERATOR_NAME = "stringappendtest";

    public static RocksDB openDB(
            String path,
            List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors,
            List<ColumnFamilyHandle> stateColumnFamilyHandles,
            ColumnFamilyOptions columnFamilyOptions,
            DBOptions dbOptions)
            throws IOException {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                new ArrayList<>(1 + stateColumnFamilyDescriptors.size());

        // we add the required descriptor for the default CF in FIRST position, see
        // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
        columnFamilyDescriptors.add(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
        columnFamilyDescriptors.addAll(stateColumnFamilyDescriptors);

        RocksDB dbRef;

        try {
            dbRef =
                    RocksDB.open(
                            Preconditions.checkNotNull(dbOptions),
                            Preconditions.checkNotNull(path),
                            columnFamilyDescriptors,
                            stateColumnFamilyHandles);
        } catch (RocksDBException e) {
            IOUtils.closeQuietly(columnFamilyOptions);
            columnFamilyDescriptors.forEach((cfd) -> IOUtils.closeQuietly(cfd.getOptions()));

            // improve error reporting on Windows
            throwExceptionIfPathLengthExceededOnWindows(path, e);

            throw new IOException("Error while opening ForSt instance.", e);
        }

        // requested + default CF
        Preconditions.checkState(
                1 + stateColumnFamilyDescriptors.size() == stateColumnFamilyHandles.size(),
                "Not all requested column family handles have been created");
        return dbRef;
    }

    /** Creates a column family handle from a state id. */
    public static ColumnFamilyHandle createColumnFamilyHandle(
            String stateId,
            RocksDB db,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory) {

        ColumnFamilyDescriptor columnFamilyDescriptor =
                createColumnFamilyDescriptor(stateId, columnFamilyOptionsFactory);

        final ColumnFamilyHandle columnFamilyHandle;
        try {
            columnFamilyHandle = createColumnFamily(columnFamilyDescriptor, db);
        } catch (Exception ex) {
            IOUtils.closeQuietly(columnFamilyDescriptor.getOptions());
            throw new FlinkRuntimeException("Error creating ColumnFamilyHandle.", ex);
        }

        return columnFamilyHandle;
    }

    /** Creates a column descriptor for a state column family. */
    public static ColumnFamilyDescriptor createColumnFamilyDescriptor(
            String stateId, Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory) {

        byte[] nameBytes = stateId.getBytes(ConfigConstants.DEFAULT_CHARSET);
        Preconditions.checkState(
                !Arrays.equals(RocksDB.DEFAULT_COLUMN_FAMILY, nameBytes),
                "The chosen state name 'default' collides with the name of the default column family!");

        ColumnFamilyOptions options =
                createColumnFamilyOptions(columnFamilyOptionsFactory, stateId);

        return new ColumnFamilyDescriptor(nameBytes, options);
    }

    private static ColumnFamilyHandle createColumnFamily(
            ColumnFamilyDescriptor columnDescriptor, RocksDB db) throws RocksDBException {
        return db.createColumnFamily(columnDescriptor);
    }

    public static ColumnFamilyOptions createColumnFamilyOptions(
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory, String stateName) {

        // ensure that we use the right merge operator, because other code relies on this
        return columnFamilyOptionsFactory
                .apply(stateName)
                .setMergeOperatorName(MERGE_OPERATOR_NAME);
    }

    @Nullable
    public static OpaqueMemoryResource<ForStSharedResources> allocateSharedCachesIfConfigured(
            ForStMemoryConfiguration jobMemoryConfig,
            Environment env,
            double memoryFraction,
            Logger logger,
            ForStMemoryControllerUtils.ForStMemoryFactory forStMemoryFactory)
            throws IOException {

        try {
            ForStSharedResourcesFactory factory =
                    ForStSharedResourcesFactory.from(jobMemoryConfig, env);
            if (factory == null) {
                return null;
            }

            return factory.create(jobMemoryConfig, env, memoryFraction, logger, forStMemoryFactory);

        } catch (Exception e) {
            throw new IOException("Failed to acquire shared cache resource for ForSt", e);
        }
    }

    private static void throwExceptionIfPathLengthExceededOnWindows(String path, Exception cause)
            throws IOException {
        // max directory path length on Windows is 247.
        // the maximum path length is 260, subtracting one file name length (12 chars) and one NULL
        // terminator.
        final int maxWinDirPathLen = 247;

        if (path.length() > maxWinDirPathLen && OperatingSystem.isWindows()) {
            throw new IOException(
                    String.format(
                            "The directory path length (%d) is longer than the directory path length limit for Windows (%d): %s",
                            path.length(), maxWinDirPathLen, path),
                    cause);
        }
    }
}
