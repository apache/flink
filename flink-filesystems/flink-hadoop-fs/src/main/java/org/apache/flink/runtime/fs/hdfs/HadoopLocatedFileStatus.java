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
 * Hadoop File Status with block locations.
 */
public class HadoopLocatedFileStatus extends HadoopFileStatus implements LocatedFileStatus {

	private HadoopBlockLocation[] blockLocations;
	/**
	 * Creates a new file status from a HDFS file status.
	 *
	 * @param fileStatus the HDFS file status
	 */
	public HadoopLocatedFileStatus(org.apache.hadoop.fs.LocatedFileStatus fileStatus) {
		super(fileStatus);
		org.apache.hadoop.fs.BlockLocation[] locations =
			((org.apache.hadoop.fs.LocatedFileStatus) getInternalFileStatus()).getBlockLocations();
		blockLocations = new HadoopBlockLocation[locations.length];
		for (int i = 0; i < locations.length; i++) {
			blockLocations[i] = new HadoopBlockLocation(locations[i]);
		}
	}

	@Override
	public BlockLocation[] getBlockLocation() {
		return blockLocations;
	}
}
