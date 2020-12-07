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

import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcColumnarRowSplitReader;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.orc.nohive.OrcNoHiveSplitReaderUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.flink.table.data.vector.VectorizedColumnBatch.DEFAULT_SIZE;

/**
 * Orc {@link SplitReader} to read files using {@link OrcColumnarRowSplitReader}.
 */
public class HiveVectorizedOrcSplitReader implements SplitReader {

	private OrcColumnarRowSplitReader reader;

	public HiveVectorizedOrcSplitReader(
			String hiveVersion,
			JobConf jobConf,
			String[] fieldNames,
			DataType[] fieldTypes,
			int[] selectedFields,
			HiveTableInputSplit split) throws IOException {
		StorageDescriptor sd = split.getHiveTablePartition().getStorageDescriptor();

		Configuration conf = new Configuration(jobConf);
		sd.getSerdeInfo().getParameters().forEach(conf::set);

		InputSplit hadoopSplit = split.getHadoopInputSplit();
		FileSplit fileSplit;
		if (hadoopSplit instanceof FileSplit) {
			fileSplit = (FileSplit) hadoopSplit;
		} else {
			throw new IllegalArgumentException("Unknown split type: " + hadoopSplit);
		}

		this.reader = hiveVersion.startsWith("1.") ?
				OrcNoHiveSplitReaderUtil.genPartColumnarRowReader(
						conf,
						fieldNames,
						fieldTypes,
						split.getHiveTablePartition().getPartitionSpec(),
						selectedFields,
						new ArrayList<>(),
						DEFAULT_SIZE,
						new Path(fileSplit.getPath().toString()),
						fileSplit.getStart(),
						fileSplit.getLength()) :
				OrcSplitReaderUtil.genPartColumnarRowReader(
						hiveVersion,
						conf,
						fieldNames,
						fieldTypes,
						split.getHiveTablePartition().getPartitionSpec(),
						selectedFields,
						new ArrayList<>(),
						DEFAULT_SIZE,
						new Path(fileSplit.getPath().toString()),
						fileSplit.getStart(),
						fileSplit.getLength());
	}

	@Override
	public void seekToRow(long rowCount, RowData reuse) throws IOException {
		this.reader.seekToRow(rowCount);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return this.reader.reachedEnd();
	}

	@Override
	public RowData nextRecord(RowData reuse) {
		return this.reader.nextRecord(reuse);
	}

	@Override
	public void close() throws IOException {
		this.reader.close();
	}
}
