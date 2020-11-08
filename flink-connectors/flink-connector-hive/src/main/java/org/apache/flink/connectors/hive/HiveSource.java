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

package org.apache.flink.connectors.hive;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.file.src.AbstractFileSource;
import org.apache.flink.connector.file.src.ContinuousEnumerationSettings;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.SimpleSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connectors.hive.read.HiveBulkFormatAdapter;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.ContinuousPartitionFetcher;
import org.apache.flink.table.filesystem.LimitableBulkFormat;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.mapred.JobConf;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.connector.file.src.FileSource.DEFAULT_SPLIT_ASSIGNER;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A unified data source that reads a hive table.
 */
public class HiveSource extends AbstractFileSource<RowData, HiveSourceSplit> implements ResultTypeQueryable<RowData> {

	private static final long serialVersionUID = 1L;

	private final JobConfWrapper jobConfWrapper;
	private final List<String> partitionKeys;
	private final ContinuousPartitionFetcher<Partition, ?> fetcher;
	private final HiveTableSource.HiveContinuousPartitionFetcherContext<?> fetcherContext;
	private final ObjectPath tablePath;

	HiveSource(
			Path[] inputPaths,
			FileEnumerator.Provider fileEnumerator,
			FileSplitAssigner.Provider splitAssigner,
			BulkFormat<RowData, HiveSourceSplit> readerFormat,
			@Nullable ContinuousEnumerationSettings continuousEnumerationSettings,
			JobConf jobConf,
			ObjectPath tablePath,
			List<String> partitionKeys,
			@Nullable ContinuousPartitionFetcher<Partition, ?> fetcher,
			@Nullable HiveTableSource.HiveContinuousPartitionFetcherContext<?> fetcherContext) {
		super(
				inputPaths,
				fileEnumerator,
				splitAssigner,
				readerFormat,
				continuousEnumerationSettings);
		this.jobConfWrapper = new JobConfWrapper(jobConf);
		this.tablePath = tablePath;
		this.partitionKeys = partitionKeys;
		this.fetcher = fetcher;
		this.fetcherContext = fetcherContext;
	}

	@Override
	public SimpleVersionedSerializer<HiveSourceSplit> getSplitSerializer() {
		return HiveSourceSplitSerializer.INSTANCE;
	}

	@Override
	public SimpleVersionedSerializer<PendingSplitsCheckpoint<HiveSourceSplit>> getEnumeratorCheckpointSerializer() {
		if (continuousPartitionedEnumerator()) {
			return new ContinuousHivePendingSplitsCheckpointSerializer(getSplitSerializer());
		} else {
			return super.getEnumeratorCheckpointSerializer();
		}
	}

	@Override
	public SplitEnumerator<HiveSourceSplit, PendingSplitsCheckpoint<HiveSourceSplit>> createEnumerator(
			SplitEnumeratorContext<HiveSourceSplit> enumContext) {
		if (continuousPartitionedEnumerator()) {
			return createContinuousSplitEnumerator(
					enumContext, fetcherContext.getConsumeStartOffset(), Collections.emptyList(), Collections.emptyList());
		} else {
			return super.createEnumerator(enumContext);
		}
	}

	@Override
	public SplitEnumerator<HiveSourceSplit, PendingSplitsCheckpoint<HiveSourceSplit>> restoreEnumerator(
			SplitEnumeratorContext<HiveSourceSplit> enumContext, PendingSplitsCheckpoint<HiveSourceSplit> checkpoint) {
		if (continuousPartitionedEnumerator()) {
			Preconditions.checkState(checkpoint instanceof ContinuousHivePendingSplitsCheckpoint,
					"Illegal type of splits checkpoint %s for streaming read partitioned table", checkpoint.getClass().getName());
			ContinuousHivePendingSplitsCheckpoint hiveCheckpoint = (ContinuousHivePendingSplitsCheckpoint) checkpoint;
			return createContinuousSplitEnumerator(
					enumContext, hiveCheckpoint.getCurrentReadOffset(), hiveCheckpoint.getSeenPartitionsSinceOffset(), hiveCheckpoint.getSplits());
		} else {
			return super.restoreEnumerator(enumContext, checkpoint);
		}
	}

	private boolean continuousPartitionedEnumerator() {
		return getBoundedness() == Boundedness.CONTINUOUS_UNBOUNDED && !partitionKeys.isEmpty();
	}

	private SplitEnumerator<HiveSourceSplit, PendingSplitsCheckpoint<HiveSourceSplit>> createContinuousSplitEnumerator(
			SplitEnumeratorContext<HiveSourceSplit> enumContext,
			Comparable<?> currentReadOffset,
			Collection<List<String>> seenPartitions,
			Collection<HiveSourceSplit> splits) {
		return new ContinuousHiveSplitEnumerator(
				enumContext,
				currentReadOffset,
				seenPartitions,
				getAssignerFactory().create(new ArrayList<>(splits)),
				getContinuousEnumerationSettings().getDiscoveryInterval().toMillis(),
				jobConfWrapper.conf(),
				tablePath,
				fetcher,
				fetcherContext
		);
	}

	/**
	 * Builder to build HiveSource instances.
	 */
	public static class HiveSourceBuilder extends AbstractFileSourceBuilder<RowData, HiveSourceSplit, HiveSourceBuilder> {

		private final JobConf jobConf;
		private final ObjectPath tablePath;
		private final List<String> partitionKeys;

		private ContinuousPartitionFetcher<Partition, ?> fetcher = null;
		private HiveTableSource.HiveContinuousPartitionFetcherContext<?> fetcherContext = null;

		HiveSourceBuilder(
				JobConf jobConf,
				ObjectPath tablePath,
				CatalogTable catalogTable,
				List<HiveTablePartition> partitions,
				@Nullable Long limit,
				String hiveVersion,
				boolean useMapRedReader,
				RowType producedRowType) {
			super(
					new Path[1],
					createBulkFormat(new JobConf(jobConf), catalogTable, hiveVersion, producedRowType, useMapRedReader, limit),
					new HiveSourceFileEnumerator.Provider(partitions, new JobConfWrapper(jobConf)),
					null);
			this.jobConf = jobConf;
			this.tablePath = tablePath;
			this.partitionKeys = catalogTable.getPartitionKeys();
		}

		@Override
		public HiveSource build() {
			FileSplitAssigner.Provider splitAssigner = continuousSourceSettings == null || partitionKeys.isEmpty() ?
					DEFAULT_SPLIT_ASSIGNER : SimpleSplitAssigner::new;
			return new HiveSource(
					inputPaths,
					fileEnumerator,
					splitAssigner,
					readerFormat,
					continuousSourceSettings,
					jobConf,
					tablePath,
					partitionKeys,
					fetcher,
					fetcherContext
			);
		}

		public HiveSourceBuilder setFetcher(ContinuousPartitionFetcher<Partition, ?> fetcher) {
			this.fetcher = fetcher;
			return this;
		}

		public HiveSourceBuilder setFetcherContext(HiveTableSource.HiveContinuousPartitionFetcherContext<?> fetcherContext) {
			this.fetcherContext = fetcherContext;
			return this;
		}

		private static BulkFormat<RowData, HiveSourceSplit> createBulkFormat(
				JobConf jobConf,
				CatalogTable catalogTable,
				String hiveVersion,
				RowType producedRowType,
				boolean useMapRedReader,
				Long limit) {
			checkNotNull(catalogTable, "catalogTable can not be null.");
			return LimitableBulkFormat.create(
					new HiveBulkFormatAdapter(
							new JobConfWrapper(jobConf),
							catalogTable.getPartitionKeys(),
							catalogTable.getSchema().getFieldNames(),
							catalogTable.getSchema().getFieldDataTypes(),
							hiveVersion,
							producedRowType,
							useMapRedReader),
					limit
			);
		}
	}
}
