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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * External resource for tests that require an instance of RocksDB.
 */
public class RocksDBResource extends ExternalResource {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBResource.class);

	/** Factory for {@link DBOptions} and {@link ColumnFamilyOptions}. */
	private final RocksDBOptionsFactory optionsFactory;

	/** Temporary folder that provides the working directory for the RocksDB instance. */
	private TemporaryFolder temporaryFolder;

	/** The options for the RocksDB instance. */
	private DBOptions dbOptions;

	/** The options for column families created with the RocksDB instance. */
	private ColumnFamilyOptions columnFamilyOptions;

	/** The options for writes. */
	private WriteOptions writeOptions;

	/** The options for reads. */
	private ReadOptions readOptions;

	/** The RocksDB instance object. */
	private RocksDB rocksDB;

	/** List of all column families that have been created with the RocksDB instance. */
	private List<ColumnFamilyHandle> columnFamilyHandles;

	/** Wrapper for batched writes to the RocksDB instance. */
	private RocksDBWriteBatchWrapper batchWrapper;

	/** Resources to close. */
	private ArrayList<AutoCloseable> handlesToClose = new ArrayList<>();

	public RocksDBResource() {
		this(new RocksDBOptionsFactory() {
			@Override
			public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
				//close it before reuse the reference.
				try {
					currentOptions.close();
				} catch (Exception e) {
					LOG.error("Close previous DBOptions's instance failed.", e);
				}

				return PredefinedOptions.FLASH_SSD_OPTIMIZED.createDBOptions(handlesToClose);
			}

			@Override
			public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
				//close it before reuse the reference.
				try {
					currentOptions.close();
				} catch (Exception e) {
					LOG.error("Close previous ColumnOptions's instance failed.", e);
				}

				return PredefinedOptions.FLASH_SSD_OPTIMIZED.createColumnOptions(handlesToClose).optimizeForPointLookup(40960);
			}
		});
	}

	public RocksDBResource(@Nonnull RocksDBOptionsFactory optionsFactory) {
		this.optionsFactory = optionsFactory;
	}

	public ColumnFamilyHandle getDefaultColumnFamily() {
		return columnFamilyHandles.get(0);
	}

	public WriteOptions getWriteOptions() {
		return writeOptions;
	}

	public RocksDB getRocksDB() {
		return rocksDB;
	}

	public ReadOptions getReadOptions() {
		return readOptions;
	}

	public RocksDBWriteBatchWrapper getBatchWrapper() {
		return batchWrapper;
	}

	/**
	 * Creates and returns a new column family with the given name.
	 */
	public ColumnFamilyHandle createNewColumnFamily(String name) {
		try {
			final ColumnFamilyHandle columnFamily = rocksDB.createColumnFamily(
				new ColumnFamilyDescriptor(name.getBytes(), columnFamilyOptions));
			columnFamilyHandles.add(columnFamily);
			return columnFamily;
		} catch (Exception ex) {
			throw new FlinkRuntimeException("Could not create column family.", ex);
		}
	}

	@Override
	protected void before() throws Throwable {
		this.temporaryFolder = new TemporaryFolder();
		this.temporaryFolder.create();
		final File rocksFolder = temporaryFolder.newFolder();
		this.dbOptions = optionsFactory.createDBOptions(
			PredefinedOptions.DEFAULT.createDBOptions(handlesToClose), handlesToClose).setCreateIfMissing(true);
		this.columnFamilyOptions = optionsFactory.createColumnOptions(
			PredefinedOptions.DEFAULT.createColumnOptions(handlesToClose), handlesToClose);
		this.writeOptions = new WriteOptions();
		this.writeOptions.disableWAL();
		this.readOptions = RocksDBOperationUtils.createTotalOrderSeekReadOptions();
		this.columnFamilyHandles = new ArrayList<>(1);
		this.rocksDB = RocksDB.open(
			dbOptions,
			rocksFolder.getAbsolutePath(),
			Collections.singletonList(new ColumnFamilyDescriptor("default".getBytes(), columnFamilyOptions)),
			columnFamilyHandles);
		this.batchWrapper = new RocksDBWriteBatchWrapper(rocksDB, writeOptions);
	}

	@Override
	protected void after() {
		// destruct in reversed order of creation.
		IOUtils.closeQuietly(this.batchWrapper);
		for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
			IOUtils.closeQuietly(columnFamilyHandle);
		}
		IOUtils.closeQuietly(this.rocksDB);
		IOUtils.closeQuietly(this.readOptions);
		IOUtils.closeQuietly(this.writeOptions);
		IOUtils.closeQuietly(this.columnFamilyOptions);
		IOUtils.closeQuietly(this.dbOptions);
		handlesToClose.forEach(IOUtils::closeQuietly);
		temporaryFolder.delete();
	}
}
