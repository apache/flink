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

package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.ValidationException;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Policy for commit a partition.
 *
 * <p>The implemented commit method needs to be idempotent because the same partition may be
 * committed multiple times.
 *
 * <p>Default implementations:
 * See {@link MetastoreCommitPolicy}.
 * See {@link SuccessFileCommitPolicy}.
 *
 * <p>Further more, you can implement your own policy, like:
 * - RPC to notify downstream applications.
 * - Trigger hive to analysis partition for generating statistics.
 * ...
 */
@Experimental
public interface PartitionCommitPolicy {

	String METASTORE = "metastore";
	String SUCCESS_FILE = "success-file";
	String CUSTOM = "custom";

	/**
	 * Commit a partition.
	 */
	void commit(Context context) throws Exception;

	/**
	 * Context of policy, including table information and partition information.
	 */
	interface Context {

		/**
		 * Catalog name of this table.
		 */
		String catalogName();

		/**
		 * Database name of this table.
		 */
		String databaseName();

		/**
		 * Table name.
		 */
		String tableName();

		/**
		 * Table partition keys.
		 */
		List<String> partitionKeys();

		/**
		 * Values of this partition.
		 */
		List<String> partitionValues();

		/**
		 * Path of this partition.
		 */
		Path partitionPath();

		/**
		 * Partition spec in the form of a map from partition keys to values.
		 */
		default LinkedHashMap<String, String> partitionSpec() {
			LinkedHashMap<String, String> res = new LinkedHashMap<>();
			for (int i = 0; i < partitionKeys().size(); i++) {
				res.put(partitionKeys().get(i), partitionValues().get(i));
			}
			return res;
		}
	}

	/**
	 * Create a policy chain from config.
	 */
	static List<PartitionCommitPolicy> createPolicyChain(
			ClassLoader cl,
			String policyKind,
			String customClass,
			String successFileName,
			Supplier<FileSystem> fsSupplier) {
		if (policyKind == null) {
			return Collections.emptyList();
		}
		String[] policyStrings = policyKind.split(",");
		return Arrays.stream(policyStrings).map(name -> {
			switch (name.toLowerCase()) {
				case METASTORE:
					return new MetastoreCommitPolicy();
				case SUCCESS_FILE:
					return new SuccessFileCommitPolicy(successFileName, fsSupplier.get());
				case CUSTOM:
					try {
						return (PartitionCommitPolicy) cl.loadClass(customClass).newInstance();
					} catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
						throw new RuntimeException(
								"Can not new instance for custom class from " + customClass, e);
					}
				default:
					throw new UnsupportedOperationException("Unsupported policy: " + name);
			}
		}).collect(Collectors.toList());
	}

	/**
	 * Validate commit policy.
	 */
	static void validatePolicyChain(boolean isEmptyMetastore, String policyKind) {
		if (policyKind != null) {
			String[] policyStrings = policyKind.split(",");
			for (String policy : policyStrings) {
				if (isEmptyMetastore && METASTORE.equalsIgnoreCase(policy)) {
					throw new ValidationException("Can not configure a 'metastore' partition commit" +
							" policy for a file system table. You can only configure 'metastore'" +
							" partition commit policy for a hive table.");
				}
			}
		}
	}
}
