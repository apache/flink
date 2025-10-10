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

package org.apache.flink.state.rocksdb.restore;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.types.Either;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ExportImportFilesMetaData;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link DistributeStateHandlerHelper}. */
public class DistributeStateHandlerHelperTest extends TestLogger {

    private static final int NUM_KEY_GROUPS = 128;
    private static final KeyGroupRange KEY_GROUP_RANGE = new KeyGroupRange(0, NUM_KEY_GROUPS - 1);
    private static final int KEY_GROUP_PREFIX_BYTES =
            CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(NUM_KEY_GROUPS);
    private static final String CF_NAME = "test-column-family";

    @TempDir private Path tempDir;

    /** Test whether sst files are exported when the key group all in range. */
    @Test
    public void testAutoCompactionIsDisabled() throws Exception {
        Path rocksDir = tempDir.resolve("rocksdb_dir");
        Path dbPath = rocksDir.resolve("db");
        Path chkDir = rocksDir.resolve("chk");
        Path exportDir = rocksDir.resolve("export");

        Files.createDirectories(dbPath);
        Files.createDirectories(exportDir);

        ArrayList<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(2);

        try (RocksDB db = openDB(dbPath.toString(), columnFamilyHandles)) {
            ColumnFamilyHandle testCfHandler = columnFamilyHandles.get(1);

            // Create SST files and verify their creation
            for (int i = 0; i < 4; i++) {
                db.flush(new FlushOptions().setWaitForFlush(true), testCfHandler);
                for (int j = 10; j < NUM_KEY_GROUPS / 2; j++) {
                    byte[] bytes = new byte[KEY_GROUP_PREFIX_BYTES];
                    CompositeKeySerializationUtils.serializeKeyGroup(j, bytes);
                    db.delete(testCfHandler, bytes);
                }
                assertThat(
                                dbPath.toFile()
                                        .listFiles(
                                                (file, name) ->
                                                        name.toLowerCase().endsWith(".sst")))
                        .hasSize(i);
            }

            // Create checkpoint
            try (Checkpoint checkpoint = Checkpoint.create(db)) {
                checkpoint.createCheckpoint(chkDir.toString());
            }
        }

        // Verify there are 4 sst files in level 0, compaction will be triggered once the DB is
        // opened.
        assertThat(chkDir.toFile().listFiles((file, name) -> name.toLowerCase().endsWith(".sst")))
                .hasSize(4);

        // Create IncrementalLocalKeyedStateHandle for testing
        IncrementalLocalKeyedStateHandle stateHandle = createTestStateHandle(chkDir.toString());

        try (DistributeStateHandlerHelper helper =
                createDistributeStateHandlerHelper(
                        stateHandle, (name) -> new ColumnFamilyOptions())) {

            // This simulates the delay that allows background compaction to clean up SST files if
            // auto compaction is enabled.
            Thread.sleep(500);
            Map<RegisteredStateMetaInfoBase.Key, List<ExportImportFilesMetaData>>
                    exportedColumnFamiliesOut = new HashMap<>();
            List<IncrementalLocalKeyedStateHandle> skipped = new ArrayList<>();

            Either<KeyGroupRange, IncrementalLocalKeyedStateHandle> result =
                    helper.tryDistribute(exportDir, exportedColumnFamiliesOut);
            assertThat(result.isLeft()).isTrue();
            assertThat(exportedColumnFamiliesOut).isNotEmpty();
            assertThat(skipped).isEmpty();
        }
    }

    private RocksDB openDB(String path, ArrayList<ColumnFamilyHandle> columnFamilyHandles)
            throws RocksDBException {

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(2);
        columnFamilyDescriptors.add(
                new ColumnFamilyDescriptor(
                        RocksDB.DEFAULT_COLUMN_FAMILY,
                        new ColumnFamilyOptions().setDisableAutoCompactions(true)));
        columnFamilyDescriptors.add(
                new ColumnFamilyDescriptor(
                        CF_NAME.getBytes(ConfigConstants.DEFAULT_CHARSET),
                        new ColumnFamilyOptions().setDisableAutoCompactions(true)));

        return RocksDB.open(
                new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true),
                path,
                columnFamilyDescriptors,
                columnFamilyHandles);
    }

    /**
     * Creates a minimal IncrementalLocalKeyedStateHandle for testing. Uses empty metadata to focus
     * on SST file distribution behavior.
     */
    private IncrementalLocalKeyedStateHandle createTestStateHandle(String checkpointDir) {
        return new IncrementalLocalKeyedStateHandle(
                UUID.randomUUID(),
                1L,
                new DirectoryStateHandle(Paths.get(checkpointDir), 0L),
                KEY_GROUP_RANGE,
                new ByteStreamStateHandle("meta", new byte[0]),
                Collections.emptyList());
    }

    /** Creates a DistributeStateHandlerHelper with test-specific configuration. */
    private DistributeStateHandlerHelper createDistributeStateHandlerHelper(
            IncrementalLocalKeyedStateHandle stateHandle,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory)
            throws Exception {
        TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
        TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

        List<StateMetaInfoSnapshot> stateMetaInfoList = new ArrayList<>();
        stateMetaInfoList.add(
                new RegisteredKeyValueStateBackendMetaInfo<>(
                                StateDescriptor.Type.VALUE,
                                CF_NAME,
                                namespaceSerializer,
                                stateSerializer)
                        .snapshot());
        return new DistributeStateHandlerHelper(
                stateHandle,
                stateMetaInfoList,
                columnFamilyOptionsFactory,
                new DBOptions().setCreateIfMissing(true),
                null,
                null,
                KEY_GROUP_PREFIX_BYTES,
                KEY_GROUP_RANGE,
                "test-operator",
                0);
    }
}
