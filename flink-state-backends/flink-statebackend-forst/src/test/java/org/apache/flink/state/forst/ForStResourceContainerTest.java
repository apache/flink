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

package org.apache.flink.state.forst;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;
import org.apache.flink.util.function.ThrowingRunnable;

import org.forstdb.BlockBasedTableConfig;
import org.forstdb.BloomFilter;
import org.forstdb.Cache;
import org.forstdb.ColumnFamilyDescriptor;
import org.forstdb.ColumnFamilyHandle;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.DBOptions;
import org.forstdb.FlinkEnv;
import org.forstdb.IndexType;
import org.forstdb.LRUCache;
import org.forstdb.NativeLibraryLoader;
import org.forstdb.ReadOptions;
import org.forstdb.RocksDB;
import org.forstdb.TableFormatConfig;
import org.forstdb.WriteBufferManager;
import org.forstdb.WriteOptions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests to guard {@link ForStResourceContainer}. */
public class ForStResourceContainerTest {

    @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    @BeforeClass
    public static void ensureForStNativeLibraryLoaded() throws IOException {
        NativeLibraryLoader.getInstance().loadLibrary(TMP_FOLDER.newFolder().getAbsolutePath());
    }

    // ------------------------------------------------------------------------

    @Test
    public void testFreeDBOptionsAfterClose() throws Exception {
        ForStResourceContainer container = new ForStResourceContainer();
        DBOptions dbOptions = container.getDbOptions();
        assertThat(dbOptions.isOwningHandle(), is(true));
        container.close();
        assertThat(dbOptions.isOwningHandle(), is(false));
    }

    @Test
    public void testFreeMultipleDBOptionsAfterClose() throws Exception {
        ForStResourceContainer container = new ForStResourceContainer();
        final int optionNumber = 20;
        ArrayList<DBOptions> dbOptions = new ArrayList<>(optionNumber);
        for (int i = 0; i < optionNumber; i++) {
            dbOptions.add(container.getDbOptions());
        }
        container.close();
        for (DBOptions dbOption : dbOptions) {
            assertThat(dbOption.isOwningHandle(), is(false));
        }
    }

    /**
     * Guard the shared resources will be released after {@link ForStResourceContainer#close()} when
     * the {@link ForStResourceContainer} instance is initiated with {@link OpaqueMemoryResource}.
     *
     * @throws Exception if unexpected error happened.
     */
    @Test
    public void testSharedResourcesAfterClose() throws Exception {
        OpaqueMemoryResource<ForStSharedResources> sharedResources = getSharedResources();
        ForStResourceContainer container = new ForStResourceContainer(null, sharedResources);
        container.close();
        ForStSharedResources forStSharedResources = sharedResources.getResourceHandle();
        assertThat(forStSharedResources.getCache().isOwningHandle(), is(false));
        assertThat(forStSharedResources.getWriteBufferManager().isOwningHandle(), is(false));
    }

    /**
     * Guard that {@link ForStResourceContainer#getDbOptions()} shares the same {@link
     * WriteBufferManager} instance if the {@link ForStResourceContainer} instance is initiated with
     * {@link OpaqueMemoryResource}.
     *
     * @throws Exception if unexpected error happened.
     */
    @Test
    public void testGetDbOptionsWithSharedResources() throws Exception {
        final int optionNumber = 20;
        OpaqueMemoryResource<ForStSharedResources> sharedResources = getSharedResources();
        ForStResourceContainer container = new ForStResourceContainer(null, sharedResources);
        HashSet<WriteBufferManager> writeBufferManagers = new HashSet<>();
        for (int i = 0; i < optionNumber; i++) {
            DBOptions dbOptions = container.getDbOptions();
            WriteBufferManager writeBufferManager = getWriteBufferManager(dbOptions);
            writeBufferManagers.add(writeBufferManager);
        }
        assertThat(writeBufferManagers.size(), is(1));
        assertThat(
                writeBufferManagers.iterator().next(),
                is(sharedResources.getResourceHandle().getWriteBufferManager()));
        container.close();
    }

    /**
     * Guard that {@link ForStResourceContainer#getColumnOptions()} shares the same {@link Cache}
     * instance if the {@link ForStResourceContainer} instance is initiated with {@link
     * OpaqueMemoryResource}.
     *
     * @throws Exception if unexpected error happened.
     */
    @Test
    public void testGetColumnFamilyOptionsWithSharedResources() throws Exception {
        final int optionNumber = 20;
        OpaqueMemoryResource<ForStSharedResources> sharedResources = getSharedResources();
        ForStResourceContainer container = new ForStResourceContainer(null, sharedResources);
        HashSet<Cache> caches = new HashSet<>();
        for (int i = 0; i < optionNumber; i++) {
            ColumnFamilyOptions columnOptions = container.getColumnOptions();
            Cache cache = getBlockCache(columnOptions);
            caches.add(cache);
        }
        assertThat(caches.size(), is(1));
        assertThat(caches.iterator().next(), is(sharedResources.getResourceHandle().getCache()));
        container.close();
    }

    private OpaqueMemoryResource<ForStSharedResources> getSharedResources() {
        final long cacheSize = 1024L, writeBufferSize = 512L;
        final LRUCache cache = new LRUCache(cacheSize, -1, false, 0.1);
        final WriteBufferManager wbm = new WriteBufferManager(writeBufferSize, cache);
        ForStSharedResources forStSharedResources =
                new ForStSharedResources(cache, wbm, writeBufferSize, false);
        return new OpaqueMemoryResource<>(
                forStSharedResources, cacheSize, forStSharedResources::close);
    }

    private Cache getBlockCache(ColumnFamilyOptions columnOptions) {
        BlockBasedTableConfig blockBasedTableConfig = null;
        try {
            blockBasedTableConfig = (BlockBasedTableConfig) columnOptions.tableFormatConfig();
        } catch (ClassCastException e) {
            fail("Table config got from ColumnFamilyOptions is not BlockBasedTableConfig");
        }
        Field cacheField = null;
        try {
            cacheField = BlockBasedTableConfig.class.getDeclaredField("blockCache");
        } catch (NoSuchFieldException e) {
            fail("blockCache is not defined");
        }
        cacheField.setAccessible(true);
        try {
            return (Cache) cacheField.get(blockBasedTableConfig);
        } catch (IllegalAccessException e) {
            fail("Cannot access blockCache field.");
            return null;
        }
    }

    private WriteBufferManager getWriteBufferManager(DBOptions dbOptions) {

        Field writeBufferManagerField = null;
        try {
            writeBufferManagerField = DBOptions.class.getDeclaredField("writeBufferManager_");
        } catch (NoSuchFieldException e) {
            fail("writeBufferManager_ is not defined.");
        }
        writeBufferManagerField.setAccessible(true);
        try {
            return (WriteBufferManager) writeBufferManagerField.get(dbOptions);
        } catch (IllegalAccessException e) {
            fail("Cannot access writeBufferManager_ field.");
            return null;
        }
    }

    @Test
    public void testFreeColumnOptionsAfterClose() throws Exception {
        ForStResourceContainer container = new ForStResourceContainer();
        ColumnFamilyOptions columnFamilyOptions = container.getColumnOptions();
        assertThat(columnFamilyOptions.isOwningHandle(), is(true));
        container.close();
        assertThat(columnFamilyOptions.isOwningHandle(), is(false));
    }

    @Test
    public void testFreeMultipleColumnOptionsAfterClose() throws Exception {
        ForStResourceContainer container = new ForStResourceContainer();
        final int optionNumber = 20;
        ArrayList<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(optionNumber);
        for (int i = 0; i < optionNumber; i++) {
            columnFamilyOptions.add(container.getColumnOptions());
        }
        container.close();
        for (ColumnFamilyOptions columnFamilyOption : columnFamilyOptions) {
            assertThat(columnFamilyOption.isOwningHandle(), is(false));
        }
    }

    @Test
    public void testFreeSharedResourcesAfterClose() throws Exception {
        LRUCache cache = new LRUCache(1024L);
        WriteBufferManager wbm = new WriteBufferManager(1024L, cache);
        ForStSharedResources sharedResources = new ForStSharedResources(cache, wbm, 1024L, false);
        final ThrowingRunnable<Exception> disposer = sharedResources::close;
        OpaqueMemoryResource<ForStSharedResources> opaqueResource =
                new OpaqueMemoryResource<>(sharedResources, 1024L, disposer);

        ForStResourceContainer container = new ForStResourceContainer(null, opaqueResource);

        container.close();
        assertThat(cache.isOwningHandle(), is(false));
        assertThat(wbm.isOwningHandle(), is(false));
    }

    @Test
    public void testFreeWriteReadOptionsAfterClose() throws Exception {
        ForStResourceContainer container = new ForStResourceContainer();
        WriteOptions writeOptions = container.getWriteOptions();
        ReadOptions readOptions = container.getReadOptions();
        assertThat(writeOptions.isOwningHandle(), is(true));
        assertThat(readOptions.isOwningHandle(), is(true));
        container.close();
        assertThat(writeOptions.isOwningHandle(), is(false));
        assertThat(readOptions.isOwningHandle(), is(false));
    }

    @Test
    public void testGetColumnFamilyOptionsWithPartitionedIndex() throws Exception {
        LRUCache cache = new LRUCache(1024L);
        WriteBufferManager wbm = new WriteBufferManager(1024L, cache);
        ForStSharedResources sharedResources = new ForStSharedResources(cache, wbm, 1024L, true);
        final ThrowingRunnable<Exception> disposer = sharedResources::close;
        OpaqueMemoryResource<ForStSharedResources> opaqueResource =
                new OpaqueMemoryResource<>(sharedResources, 1024L, disposer);
        BloomFilter blockBasedFilter = new BloomFilter();
        ForStOptionsFactory blockBasedBloomFilterOptionFactory =
                new ForStOptionsFactory() {

                    @Override
                    public DBOptions createDBOptions(
                            DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                        return currentOptions;
                    }

                    @Override
                    public ColumnFamilyOptions createColumnOptions(
                            ColumnFamilyOptions currentOptions,
                            Collection<AutoCloseable> handlesToClose) {
                        TableFormatConfig tableFormatConfig = currentOptions.tableFormatConfig();
                        BlockBasedTableConfig blockBasedTableConfig =
                                tableFormatConfig == null
                                        ? new BlockBasedTableConfig()
                                        : (BlockBasedTableConfig) tableFormatConfig;
                        blockBasedTableConfig.setFilter(blockBasedFilter);
                        handlesToClose.add(blockBasedFilter);
                        currentOptions.setTableFormatConfig(blockBasedTableConfig);
                        return currentOptions;
                    }
                };
        try (ForStResourceContainer container =
                new ForStResourceContainer(blockBasedBloomFilterOptionFactory, opaqueResource)) {
            ColumnFamilyOptions columnOptions = container.getColumnOptions();
            BlockBasedTableConfig actual =
                    (BlockBasedTableConfig) columnOptions.tableFormatConfig();
            assertThat(actual.indexType(), is(IndexType.kTwoLevelIndexSearch));
            assertThat(actual.partitionFilters(), is(true));
            assertThat(actual.pinTopLevelIndexAndFilter(), is(true));
            assertFalse(actual.filterPolicy() == blockBasedFilter);
        }
        assertFalse("Block based filter is left unclosed.", blockBasedFilter.isOwningHandle());
    }

    @Test
    public void testDirectoryResources() throws Exception {
        File localBasePath = TMP_FOLDER.newFolder();
        Path remoteBasePath = new Path(TMP_FOLDER.newFolder().getPath());
        try (final ForStResourceContainer optionsContainer =
                new ForStResourceContainer(
                        new Configuration(), null, null, localBasePath, remoteBasePath, false)) {
            optionsContainer.prepareDirectories();
            assertTrue(localBasePath.exists());
            assertTrue(new File(remoteBasePath.getPath()).exists());
            assertTrue(optionsContainer.getDbOptions().getEnv() instanceof FlinkEnv);

            optionsContainer.clearDirectories();
            assertFalse(localBasePath.exists());
            assertFalse(new File(remoteBasePath.getPath()).exists());
        }
    }

    @Test
    public void testFileSystemInit() throws Exception {
        File localBasePath = TMP_FOLDER.newFolder();
        Path remoteBasePath = new Path(TMP_FOLDER.newFolder().getPath());
        ArrayList<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
        ArrayList<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(1);
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        DBOptions dbOptions2 =
                new DBOptions().setCreateIfMissing(true).setAvoidFlushDuringShutdown(true);
        ForStFlinkFileSystem.setupLocalBasePath(remoteBasePath.toString(), localBasePath.getPath());
        FileSystem forstFileSystem = ForStFlinkFileSystem.get(remoteBasePath.toUri());
        dbOptions2.setEnv(new FlinkEnv(remoteBasePath.toString(), forstFileSystem));
        RocksDB db =
                RocksDB.open(
                        dbOptions2,
                        remoteBasePath.getPath(),
                        columnFamilyDescriptors,
                        columnFamilyHandles);
        db.put("key".getBytes(), "value".getBytes());
        db.getSnapshot();
        db.close();
    }
}
