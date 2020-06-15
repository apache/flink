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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.hive.ConsumeOrder;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.connectors.hive.HiveTableSource;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.filesystem.PartitionTimeExtractor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.flink.table.filesystem.DefaultPartTimeExtractor.toMills;

/**
 * This is the single (non-parallel) monitoring task which takes a {@link HiveTableInputFormat},
 * it is responsible for:
 *
 * <ol>
 *     <li>Monitoring partitions of hive meta store.</li>
 *     <li>Deciding which partitions should be further read and processed.</li>
 *     <li>Creating the {@link HiveTableInputSplit splits} corresponding to those partitions.</li>
 *     <li>Assigning them to downstream tasks for further processing.</li>
 * </ol>
 *
 * <p>The splits to be read are forwarded to the downstream {@link ContinuousFileReaderOperator}
 * which can have parallelism greater than one.
 *
 * <p><b>IMPORTANT NOTE: </b> Splits are forwarded downstream for reading in ascending partition time order,
 * based on the partition time of the partitions they belong to.
 */
public class HiveContinuousMonitoringFunction
		extends RichSourceFunction<TimestampedHiveInputSplit>
		implements CheckpointedFunction {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(HiveContinuousMonitoringFunction.class);

	/** The parallelism of the downstream readers. */
	private final int readerParallelism;

	/** The interval between consecutive path scans. */
	private final long interval;

	private final HiveShim hiveShim;

	private final JobConfWrapper conf;

	private final ObjectPath tablePath;

	private final List<String> partitionKeys;

	private final String[] fieldNames;

	private final DataType[] fieldTypes;

	// consumer variables
	private final ConsumeOrder consumeOrder;
	private final String consumeOffset;

	// extractor variables
	private final String extractorKind;
	private final String extractorClass;
	private final String extractorPattern;

	private volatile boolean isRunning = true;

	/** The maximum partition read time seen so far. */
	private volatile long currentReadTime;

	private transient PartitionDiscovery.Context context;

	private transient PartitionDiscovery fetcher;

	private transient Object checkpointLock;

	private transient ListState<Long> currReadTimeState;

	private transient ListState<List<List<String>>> distinctPartsState;

	private transient IMetaStoreClient client;

	private transient Properties tableProps;

	private transient String defaultPartitionName;

	private transient Set<List<String>> distinctPartitions;

	public HiveContinuousMonitoringFunction(
			HiveShim hiveShim,
			JobConf conf,
			ObjectPath tablePath,
			CatalogTable catalogTable,
			int readerParallelism,
			ConsumeOrder consumeOrder,
			String consumeOffset,
			String extractorKind,
			String extractorClass,
			String extractorPattern,
			long interval) {
		this.hiveShim = hiveShim;
		this.conf = new JobConfWrapper(conf);
		this.tablePath = tablePath;
		this.partitionKeys = catalogTable.getPartitionKeys();
		this.fieldNames = catalogTable.getSchema().getFieldNames();
		this.fieldTypes = catalogTable.getSchema().getFieldDataTypes();
		this.consumeOrder = consumeOrder;
		this.extractorKind = extractorKind;
		this.extractorClass = extractorClass;
		this.extractorPattern = extractorPattern;
		this.consumeOffset = consumeOffset;

		this.interval = interval;
		this.readerParallelism = Math.max(readerParallelism, 1);
		this.currentReadTime = 0;
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		this.currReadTimeState = context.getOperatorStateStore().getListState(
			new ListStateDescriptor<>(
				"current-read-time-state",
				LongSerializer.INSTANCE
			)
		);
		this.distinctPartsState = context.getOperatorStateStore().getListState(
			new ListStateDescriptor<>(
				"distinct-partitions-state",
				new ListSerializer<>(new ListSerializer<>(StringSerializer.INSTANCE))
			)
		);

		this.client = this.hiveShim.getHiveMetastoreClient(new HiveConf(conf.conf(), HiveConf.class));

		Table hiveTable = client.getTable(tablePath.getDatabaseName(), tablePath.getObjectName());
		this.tableProps = HiveReflectionUtils.getTableMetadata(hiveShim, hiveTable);
		this.defaultPartitionName = conf.conf().get(HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname,
				HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal);

		PartitionTimeExtractor extractor = PartitionTimeExtractor.create(
				getRuntimeContext().getUserCodeClassLoader(),
				extractorKind,
				extractorClass,
				extractorPattern);

		this.fetcher = new DirectoryMonitorDiscovery();

		Path location = new Path(hiveTable.getSd().getLocation());
		FileSystem fs = location.getFileSystem(conf.conf());
		this.context = new PartitionDiscovery.Context() {

			@Override
			public List<String> partitionKeys() {
				return partitionKeys;
			}

			@Override
			public Optional<Partition> getPartition(List<String> partValues) throws TException {
				try {
					return Optional.of(client.getPartition(
							tablePath.getDatabaseName(),
							tablePath.getObjectName(),
							partValues));
				} catch (NoSuchObjectException e) {
					return Optional.empty();
				}
			}

			@Override
			public FileSystem fileSystem() {
				return fs;
			}

			@Override
			public Path tableLocation() {
				return new Path(hiveTable.getSd().getLocation());
			}

			@Override
			public long extractTimestamp(
					List<String> partKeys,
					List<String> partValues,
					Supplier<Long> fileTime) {
				switch (consumeOrder) {
					case CREATE_TIME_ORDER:
						return fileTime.get();
					case PARTITION_TIME_ORDER:
						return toMills(extractor.extract(partKeys, partValues));
					default:
						throw new UnsupportedOperationException(
								"Unsupported consumer order: " + consumeOrder);
				}
			}
		};

		this.distinctPartitions = new HashSet<>();
		if (context.isRestored()) {
			LOG.info("Restoring state for the {}.", getClass().getSimpleName());
			this.currentReadTime = this.currReadTimeState.get().iterator().next();
			this.distinctPartitions.addAll(this.distinctPartsState.get().iterator().next());
		} else {
			LOG.info("No state to restore for the {}.", getClass().getSimpleName());
			this.currentReadTime = toMills(consumeOffset);
		}
	}

	@Override
	public void run(SourceContext<TimestampedHiveInputSplit> context) throws Exception {
		checkpointLock = context.getCheckpointLock();
		while (isRunning) {
			synchronized (checkpointLock) {
				monitorAndForwardSplits(context);
			}
			Thread.sleep(interval);
		}
	}

	private void monitorAndForwardSplits(
			SourceContext<TimestampedHiveInputSplit> context) throws Exception {
		assert (Thread.holdsLock(checkpointLock));

		List<Tuple2<Partition, Long>> partitions = fetcher.fetchPartitions(this.context, currentReadTime);

		if (partitions.isEmpty()) {
			return;
		}

		partitions.sort((o1, o2) -> (int) (o1.f1 - o2.f1));

		long maxTimestamp = Long.MIN_VALUE;
		Set<List<String>> nextDistinctParts = new HashSet<>();
		for (Tuple2<Partition, Long> tuple2 : partitions) {
			Partition partition = tuple2.f0;
			List<String> partSpec = partition.getValues();
			if (!this.distinctPartitions.contains(partSpec)) {
				this.distinctPartitions.add(partSpec);
				long timestamp = tuple2.f1;
				if (timestamp > currentReadTime) {
					nextDistinctParts.add(partSpec);
				}
				if (timestamp > maxTimestamp) {
					maxTimestamp = timestamp;
				}
				LOG.info("Found new partition {} of table {}, forwarding splits to downstream readers",
						partSpec, tablePath.getFullName());
				HiveTableInputSplit[] splits = HiveTableInputFormat.createInputSplits(
						this.readerParallelism,
						Collections.singletonList(toHiveTablePartition(partition)),
						this.conf.conf());
				for (HiveTableInputSplit split : splits) {
					context.collect(new TimestampedHiveInputSplit(timestamp, split));
				}
			}
		}

		if (maxTimestamp > currentReadTime) {
			currentReadTime = maxTimestamp;
			distinctPartitions.clear();
			distinctPartitions.addAll(nextDistinctParts);
		}
	}

	private HiveTablePartition toHiveTablePartition(Partition p) {
		return HiveTableSource.toHiveTablePartition(
				partitionKeys, fieldNames, fieldTypes, hiveShim, tableProps, defaultPartitionName, p);
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkState(this.currReadTimeState != null,
				"The " + getClass().getSimpleName() + " state has not been properly initialized.");

		this.currReadTimeState.clear();
		this.currReadTimeState.add(this.currentReadTime);

		this.distinctPartsState.clear();
		this.distinctPartsState.add(new ArrayList<>(this.distinctPartitions));

		if (LOG.isDebugEnabled()) {
			LOG.debug("{} checkpointed {}.", getClass().getSimpleName(), currentReadTime);
		}
	}

	@Override
	public void close() {
		cancel();
	}

	@Override
	public void cancel() {
		// this is to cover the case where cancel() is called before the run()
		if (checkpointLock != null) {
			synchronized (checkpointLock) {
				currentReadTime = Long.MAX_VALUE;
				isRunning = false;
			}
		} else {
			currentReadTime = Long.MAX_VALUE;
			isRunning = false;
		}

		if (this.client != null) {
			this.client.close();
			this.client = null;
		}
	}
}
