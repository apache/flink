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

package org.apache.flink.table.filesystem.stream;

import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_DELAY;

/**
 * Partition commit trigger by path creation time and processing time service,
 * if 'current processing time' > 'partition directory creation time' + 'delay', will commit the partition.
 *
 * <p>It is hard to get partition start time from writer, so just get creation time from file system.
 */
public class ProcTimeCommitTigger extends PartitionCommitTrigger {

	private final long commitDelay;
	private final ProcessingTimeService procTimeService;
	private final FileSystem fileSystem;
	private final Path locationPath;

	public ProcTimeCommitTigger(
			boolean isRestored,
			OperatorStateStore stateStore,
			Configuration conf,
			ProcessingTimeService procTimeService,
			FileSystem fileSystem,
			Path locationPath) throws Exception {
		super(isRestored, stateStore);
		this.procTimeService = procTimeService;
		this.fileSystem = fileSystem;
		this.locationPath = locationPath;
		this.commitDelay = conf.get(SINK_PARTITION_COMMIT_DELAY).toMillis();
	}

	@Override
	public List<String> committablePartitions(long checkpointId) throws IOException {
		List<String> needCommit = new ArrayList<>();
		long currentProcTime = procTimeService.getCurrentProcessingTime();
		Iterator<String> iter = pendingPartitions.iterator();
		while (iter.hasNext()) {
			String partition = iter.next();
			long creationTime = fileSystem.getFileStatus(new Path(locationPath, partition))
					.getModificationTime();
			if (currentProcTime > creationTime + commitDelay) {
				needCommit.add(partition);
				iter.remove();
			}
		}
		return needCommit;
	}
}
