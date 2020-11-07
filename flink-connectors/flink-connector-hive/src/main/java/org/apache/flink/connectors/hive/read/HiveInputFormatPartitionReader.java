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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.PartitionReader;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.List;

/**
 * Partition reader that read records from InputFormat using given partitions.
 */
public class HiveInputFormatPartitionReader implements PartitionReader<HiveTablePartition, RowData> {

	private static final long serialVersionUID = 1L;
	private final JobConfWrapper jobConfWrapper;
	private final String hiveVersion;
	protected final ObjectPath tablePath;
	private final DataType[] fieldTypes;
	private final String[] fieldNames;
	private final List<String> partitionKeys;
	private final int[] selectedFields;
	private final boolean useMapRedReader;

	private transient HiveTableInputFormat hiveTableInputFormat;
	private transient HiveTableInputSplit[] inputSplits;
	private transient int readingSplitId;

	public HiveInputFormatPartitionReader(
			JobConf jobConf,
			String hiveVersion,
			ObjectPath tablePath,
			DataType[] fieldTypes,
			String[] fieldNames,
			List<String> partitionKeys,
			int[] selectedFields,
			boolean useMapRedReader) {
		this.jobConfWrapper = new JobConfWrapper(jobConf);
		this.hiveVersion = hiveVersion;
		this.tablePath = tablePath;
		this.fieldTypes = fieldTypes;
		this.fieldNames = fieldNames;
		this.partitionKeys = partitionKeys;
		this.selectedFields = selectedFields;
		this.useMapRedReader = useMapRedReader;
	}

	@Override
	public void open(List<HiveTablePartition> partitions) throws IOException {
		hiveTableInputFormat = new HiveTableInputFormat(
				this.jobConfWrapper.conf(),
				this.partitionKeys,
				this.fieldTypes,
				this.fieldNames,
				this.selectedFields,
				null,
				this.hiveVersion,
				this.useMapRedReader,
				partitions);
		inputSplits = hiveTableInputFormat.createInputSplits(1);
		readingSplitId = 0;
		if (inputSplits.length > 0) {
			hiveTableInputFormat.open(inputSplits[readingSplitId]);
		}
	}

	@Override
	public RowData read(RowData reuse) throws IOException {
		while (hasNext()) {
			return hiveTableInputFormat.nextRecord(reuse);
		}
		return null;
	}

	private boolean hasNext() throws IOException {
		if (inputSplits.length > 0) {
			if (hiveTableInputFormat.reachedEnd() && readingSplitId == inputSplits.length - 1) {
				return false;
			} else if (hiveTableInputFormat.reachedEnd()) {
				readingSplitId++;
				hiveTableInputFormat.open(inputSplits[readingSplitId]);
			}
			return true;
		}
		return false;
	}

	@Override
	public void close() throws IOException {
		hiveTableInputFormat.close();
	}
}
