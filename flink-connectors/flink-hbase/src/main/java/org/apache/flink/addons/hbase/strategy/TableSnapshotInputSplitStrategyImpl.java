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

package org.apache.flink.addons.hbase.strategy;

import org.apache.flink.addons.hbase.TableInputSplit;
import org.apache.flink.addons.hbase.util.HBaseConnectorUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.ClientSideRegionScanner;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Table input split strategy for snapshot based HTable scanning.
 */
public class TableSnapshotInputSplitStrategyImpl extends AbstractTableInputSplitStrategy
	implements TableInputSplitStrategy {

	protected static final Logger LOG = LoggerFactory.getLogger(TableSnapshotInputSplitStrategyImpl.class);

	/**
	 * Key for specifying the snapshot name.
	 *
	 * <p>This is used in {@link TableSnapshotInputFormatImpl#setInput(Configuration, String, Path)}.
	 */
	private static final String SNAPSHOT_NAME_KEY = "hbase.TableSnapshotInputFormat.snapshot.name";

	/**
	 * Key for specifying the root dir of the restored snapshot.
	 *
	 * <p>This is used in {@link TableSnapshotInputFormatImpl#setInput(Configuration, String, Path)}
	 */
	protected static final String RESTORE_DIR_KEY = "hbase.TableSnapshotInputFormat.restore.dir";

	/**
	 * The restore path for a snapshot.
	 */
	private String restoreDirPath;

	/**
	 * This is the child directory of {@link #restoreDirPath}. For example, if {@link #restoreDirPath} is
	 * <tt>hdfs://yourdomain/my_restore_path</tt>, then <tt>restoreSnapshotPath</tt>
	 * could be <tt>hdfs://yourdomain/my_restore_path/f761da7d-7af1-473b-a034-f73633371bf6</tt>
	 */
	private String restoreSnapshotPath;

	/**
	 * The snapshot name.
	 */
	private String snapshotName;

	/**
	 * The HBase root directory.
	 *
	 * <p>Usually this value can left to null, for the following scenarios, this value will
	 * overwrite value in HBase configuration in classpath.
	 * <ul>
	 * <li>1. Unit testing.</li>
	 * <li>2. Environment that does not have <tt>hbase-site.xml</tt> in classpath.</li>
	 * </ul>
	 */
	private String hbaseRootDir;

	/**
	 * If this class has been configured.
	 */
	private boolean hasConfigured;

	/**
	 * Creates a new table snapshot input split strategy.
	 *
	 * @param tableName      the HBase table name.
	 * @param snapshotName   the snapshot name.
	 * @param restoreDirPath the restore directory path.
	 */
	public TableSnapshotInputSplitStrategyImpl(String tableName, String snapshotName, String restoreDirPath) {
		this.tableName = tableName;
		this.snapshotName = snapshotName;
		this.restoreDirPath = restoreDirPath;
	}

	/**
	 * Creates a new table snapshot input split strategy.
	 *
	 * @param tableName      the HBase table name.
	 * @param snapshotName   the snapshot name.
	 * @param restoreDirPath the restore directory path.
	 * @param hbaseRootDir   the hbase root directory.
	 */
	public TableSnapshotInputSplitStrategyImpl(String tableName, String snapshotName, String restoreDirPath, String hbaseRootDir) {
		this.tableName = tableName;
		this.snapshotName = snapshotName;
		this.restoreDirPath = restoreDirPath;
		this.hbaseRootDir = hbaseRootDir;
	}

	/**
	 * Configuration that needs to be done.
	 *
	 * <p>{@link #restoreSnapshotPath} is generated based on {@link #restoreDirPath}.
	 * If the {@link #restoreSnapshotPath} is generated, the value will be serialized and transferred.
	 *
	 * @param table the HTable.
	 * @param scan  the scanner.
	 * @throws IOException
	 */
	@Override
	public void configure(HTable table, Scan scan) throws IOException {
		super.configure(table, scan);
		if (!hasConfigured) {
			Path restoreDir = new Path(restoreDirPath, UUID.randomUUID().toString());
			restoreSnapshotPath = restoreDir.toUri().toString();
			hasConfigured = true;
		}
	}

	@Override
	public TableInputSplit[] createInputSplits(Configuration hbaseConfiguration, int minNumSplits) throws IOException {
		preCheck();
		prepareConfiguration(hbaseConfiguration);

		restoreSnapshot(hbaseConfiguration);

		hbaseConfiguration.set(SNAPSHOT_NAME_KEY, snapshotName);
		hbaseConfiguration.set(RESTORE_DIR_KEY, restoreSnapshotPath);
		AtomicInteger splitNumber = new AtomicInteger(0);
		List<TableSnapshotInputFormatImpl.InputSplit> inputSplits = TableSnapshotInputFormatImpl.getSplits(hbaseConfiguration);
		List<TableInputSplit> splits = new ArrayList<>(inputSplits.size());
		for (TableSnapshotInputFormatImpl.InputSplit split : inputSplits) {
			splits.add(new TableInputSplit(splitNumber.getAndIncrement(), split.getLocations(),
				split.getRegionInfo().getTable().getName(), split.getRegionInfo().getStartKey(),
				split.getRegionInfo().getEndKey(), split.getRegionInfo()));
		}

		LOG.info("Created " + splits.size() + " splits");
		for (TableInputSplit split : splits) {
			HBaseConnectorUtil.logSplitInfo("created", split, this.getClass().getName());
		}

		return splits.toArray(new TableInputSplit[splits.size()]);
	}

	/**
	 * Restore snapshot.
	 *
	 * @param hbaseConfiguration hbase configuration.
	 * @throws IOException
	 */
	private void restoreSnapshot(Configuration hbaseConfiguration) throws IOException {
		Path rootDir = FSUtils.getRootDir(hbaseConfiguration);
		FileSystem fs = rootDir.getFileSystem(hbaseConfiguration);
		Path restoreDir = new Path(restoreSnapshotPath);
		LOG.info("tableName={}, snapshotName={}, restoreSnapshotPath={}", tableName, snapshotName, restoreSnapshotPath);
		RestoreSnapshotHelper.copySnapshotForScanner(hbaseConfiguration, fs, rootDir, restoreDir, snapshotName);
		LOG.info("Restore snapshot {} for scanner successfully", snapshotName);
	}

	/**
	 * Create {@link ClientSideRegionScanner} as {@link ResultScanner}. A client scanner will
	 * be used to scan a read-only immutable HRegion.
	 *
	 * @param split table input split
	 * @return client side result scanner
	 * @throws IOException
	 */
	@Override
	public ResultScanner createResultScanner(Configuration hbaseConfiguration, TableInputSplit split)
		throws IOException {
		prepareConfiguration(hbaseConfiguration);

		scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
		scan.setCacheBlocks(false);

		Path rootDir = FSUtils.getRootDir(hbaseConfiguration);
		FileSystem fs = rootDir.getFileSystem(hbaseConfiguration);

		HRegionInfo hRegionInfo = new HRegionInfo(table.getName(),
			split.getStartRow(), split.getEndRow(), split.isSplit(), split.getRegionId(), split.getReplicaId());
		Path restoreDir = new Path(restoreSnapshotPath);
		return new ClientSideRegionScanner(table.getConfiguration(), fs, restoreDir, table.getTableDescriptor(),
			hRegionInfo, scan, null);
	}

	/**
	 * Prepare HBase configuration.
	 *
	 * <p>If {@link #hbaseRootDir} is set, then overwrite <tt>hbase.rootdir</tt> in configuration.
	 * Also, some mandatory key values will be set.
	 *
	 * @param configuration the HBase configuration.
	 * @return the HBase configuration.
	 * @throws IOException
	 */
	private Configuration prepareConfiguration(Configuration configuration) throws IOException {
		if (StringUtils.isNotEmpty(hbaseRootDir)) {
			configuration.set(HConstants.HBASE_DIR, hbaseRootDir);
		}
		LOG.info("HBase root.dir is set to {}", configuration.get(HConstants.HBASE_DIR));
		Path rootDir = FSUtils.getRootDir(configuration);
		configuration.set("fs.default.name", rootDir.getFileSystem(configuration).getUri().toString());
		configuration.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, tableName);
		configuration.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN, convertScanToString(scan));
		return configuration;
	}

	/**
	 * Writes the given scan into a Base64 encoded string.
	 *
	 * @param scan The scan to write out.
	 * @return The scan saved in a Base64 encoded string.
	 * @throws IOException When writing the scan fails.
	 */
	private String convertScanToString(Scan scan) throws IOException {
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		return Base64.encodeBytes(proto.toByteArray());
	}
}
