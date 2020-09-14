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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.LocatedFileStatus;

/**
 * Concrete implementation of the {@link LocatedFileStatus} interface for the
 * Hadoop Distributed File System.
 */
public final class LocatedHadoopFileStatus extends HadoopFileStatus implements LocatedFileStatus {

	/**
	 * Creates a new located file status from an HDFS file status.
	 */
	public LocatedHadoopFileStatus(org.apache.hadoop.fs.LocatedFileStatus fileStatus) {
		super(fileStatus);
	}

	@Override
	public BlockLocation[] getBlockLocations() {
		final org.apache.hadoop.fs.BlockLocation[] hadoopLocations =
				((org.apache.hadoop.fs.LocatedFileStatus) getInternalFileStatus()).getBlockLocations();

		final HadoopBlockLocation[] locations = new HadoopBlockLocation[hadoopLocations.length];
		for (int i = 0; i < locations.length; i++) {
			locations[i] = new HadoopBlockLocation(hadoopLocations[i]);
		}
		return locations;
	}
}
