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

package org.apache.flink.table.filesystem.streaming.policy;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.filesystem.TableMetaStoreFactory;
import org.apache.flink.table.filesystem.streaming.FileSystemStreamingSink;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Policy for partition commit.
 */
public interface PartitionCommitPolicy extends Serializable {

	/**
	 * Commit partition by partitionSpec and path.
	 */
	void commit(
			LinkedHashMap<String, String> partitionSpec,
			Path partitionPath,
			FileSystem fileSystem,
			TableMetaStoreFactory.TableMetaStore metaStore) throws Exception;

	static List<PartitionCommitPolicy> createCommitChain(Map<String, String> properties) {
		String policy = properties.get(
				FileSystemStreamingSink.CONNECTOR_SINK_PARTITION_COMMIT_POLICY);
		if (policy == null) {
			return Collections.emptyList();
		}
		String[] policyStrings = policy.split(",");
		return Arrays.stream(policyStrings).map(name -> {
			switch (name) {
				case "metastore":
					return new MetastoreCommitPolicy();
				case "success-file":
					String fileName = properties.get(
							FileSystemStreamingSink.CONNECTOR_SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME);
					return new SuccessFileCommitPolicy(fileName == null ? "_SUCCESS" : fileName);
				default:
					throw new UnsupportedOperationException("Unsupported policy: " + name);
			}
		}).collect(Collectors.toList());
	}
}
