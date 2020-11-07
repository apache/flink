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
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.hive.HiveTableSource.HiveContinuousPartitionFetcherContext;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.filesystem.ContinuousPartitionFetcher;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
 *
 * @param <T> The serializer type of partition offset.
 */
public class HiveContinuousMonitoringFunction<T extends Comparable>
		extends RichSourceFunction<TimestampedHiveInputSplit>
		implements CheckpointedFunction {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(HiveContinuousMonitoringFunction.class);

	private final ContinuousPartitionFetcher<Partition, T> fetcher;
	private final HiveContinuousPartitionFetcherContext<T> fetcherContext;

	/** The parallelism of the downstream readers. */
	private final int readerParallelism;

	/** The interval between consecutive path scans. */
	private final long interval;
	private ObjectPath tablePath;
	private final JobConfWrapper conf;

	private volatile boolean isRunning = true;

	/** The maximum partition read offset seen so far. */
	private volatile T currentReadOffset;

	private transient Object checkpointLock;

	private transient ListState<T> currReadOffsetState;

	private transient ListState<List<List<String>>> distinctPartsState;

	private transient Set<List<String>> distinctPartitions;

	public HiveContinuousMonitoringFunction(
			ContinuousPartitionFetcher<Partition, T> fetcher,
			HiveContinuousPartitionFetcherContext<T> fetcherContext,
			JobConf conf,
			ObjectPath tablePath,
			int readerParallelism,
			long interval) {
		this.conf = new JobConfWrapper(conf);
		this.tablePath = tablePath;
		this.interval = interval;
		this.readerParallelism = Math.max(readerParallelism, 1);
		this.fetcher = fetcher;
		this.fetcherContext = fetcherContext;
		this.currentReadOffset = fetcherContext.getConsumeStartOffset();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		this.fetcherContext.open();

		this.currReadOffsetState = context.getOperatorStateStore().getListState(
			new ListStateDescriptor<>(
				"current-read-offset-state",
				fetcherContext.getTypeSerializer()
			)
		);
		this.distinctPartsState = context.getOperatorStateStore().getListState(
			new ListStateDescriptor<>(
				"distinct-partitions-state",
				new ListSerializer<>(new ListSerializer<>(StringSerializer.INSTANCE))
			)
		);

		this.distinctPartitions = new HashSet<>();
		if (context.isRestored()) {
			LOG.info("Restoring state for the {}.", getClass().getSimpleName());
			this.currentReadOffset = this.currReadOffsetState.get().iterator().next();
			this.distinctPartitions.addAll(this.distinctPartsState.get().iterator().next());
		} else {
			LOG.info("No state to restore for the {}.", getClass().getSimpleName());
			this.currentReadOffset = fetcherContext.getConsumeStartOffset();
		}
	}

	@Override
	public void run(SourceContext<TimestampedHiveInputSplit> context) throws Exception {
		checkpointLock = context.getCheckpointLock();
		while (isRunning) {
			synchronized (checkpointLock) {
				if (isRunning) {
					monitorAndForwardSplits(context);
				}
			}
			Thread.sleep(interval);
		}
	}

	private void monitorAndForwardSplits(
			SourceContext<TimestampedHiveInputSplit> context) throws Exception {
		assert (Thread.holdsLock(checkpointLock));

		List<Tuple2<Partition, T>> partitions = fetcher.fetchPartitions(this.fetcherContext, currentReadOffset);
		if (partitions.isEmpty()) {
			return;
		}

		partitions.sort(Comparator.comparing(o -> o.f1));

		T maxPartitionOffset = null;
		Set<List<String>> nextDistinctParts = new HashSet<>();
		for (Tuple2<Partition, T> tuple2 : partitions) {
			Partition partition = tuple2.f0;
			List<String> partSpec = partition.getValues();
			if (!this.distinctPartitions.contains(partSpec)) {
				this.distinctPartitions.add(partSpec);
				T partitionOffset = tuple2.f1;
				if (partitionOffset.compareTo(currentReadOffset) > 0) {
					nextDistinctParts.add(partSpec);
				}
				if (maxPartitionOffset == null || partitionOffset.compareTo(maxPartitionOffset) > 0) {
					maxPartitionOffset = partitionOffset;
				}
				LOG.info("Found new partition {} of table {}, forwarding splits to downstream readers",
						partSpec, tablePath.getFullName());
				HiveTableInputSplit[] splits = HiveTableInputFormat.createInputSplits(
						this.readerParallelism,
						Collections.singletonList(fetcherContext.toHiveTablePartition(partition)),
						this.conf.conf());
				long modificationTime = fetcherContext.getModificationTime(partition, tuple2.f1);
				for (HiveTableInputSplit split : splits) {
					context.collect(new TimestampedHiveInputSplit(modificationTime, split));
				}
			}
		}

		if (maxPartitionOffset != null || maxPartitionOffset.compareTo(currentReadOffset) > 0) {
			currentReadOffset = maxPartitionOffset;
			distinctPartitions.clear();
			distinctPartitions.addAll(nextDistinctParts);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		Preconditions.checkState(this.currReadOffsetState != null,
				"The " + getClass().getSimpleName() + " state has not been properly initialized.");

		this.currReadOffsetState.clear();
		this.currReadOffsetState.add(this.currentReadOffset);

		this.distinctPartsState.clear();
		this.distinctPartsState.add(new ArrayList<>(this.distinctPartitions));

		if (LOG.isDebugEnabled()) {
			LOG.debug("{} checkpointed {}.", getClass().getSimpleName(), currentReadOffset);
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
				isRunning = false;
			}
		} else {
			isRunning = false;
		}
		try {
			if (this.fetcherContext != null) {
				this.fetcherContext.close();
			}
		} catch (Exception e) {
			throw new TableException("failed to close the context.");
		}
	}
}
