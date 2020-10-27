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

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.core.fs.Path;

import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link FileEnumerator} implementation for hive source, which generates splits based on {@link HiveTablePartition}s.
 */
public class HiveSourceFileEnumerator implements FileEnumerator {

	// For non-partition hive table, partitions only contains one partition which partitionValues is empty.
	private final List<HiveTablePartition> partitions;
	private final JobConf jobConf;

	public HiveSourceFileEnumerator(List<HiveTablePartition> partitions, JobConf jobConf) {
		this.partitions = partitions;
		this.jobConf = jobConf;
	}

	@Override
	public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits) throws IOException {
		return new ArrayList<>(HiveSource.createInputSplits(minDesiredSplits, partitions, jobConf));
	}

	/**
	 * A factory to create {@link HiveSourceFileEnumerator}.
	 */
	public static class Provider implements FileEnumerator.Provider {

		private static final long serialVersionUID = 1L;

		private final List<HiveTablePartition> partitions;
		private final JobConfWrapper jobConfWrapper;

		public Provider(List<HiveTablePartition> partitions, JobConfWrapper jobConfWrapper) {
			this.partitions = partitions;
			this.jobConfWrapper = jobConfWrapper;
		}

		@Override
		public FileEnumerator create() {
			return new HiveSourceFileEnumerator(partitions, jobConfWrapper.conf());
		}
	}
}
