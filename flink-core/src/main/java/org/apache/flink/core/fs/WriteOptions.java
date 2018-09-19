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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Write options that can be passed to the methods that write files.
 */
@Public
public class WriteOptions {

	private WriteMode overwrite = WriteMode.NO_OVERWRITE;

	@Nullable
	private BlockOptions blockSettings;

	private boolean injectEntropy;

	// ------------------------------------------------------------------------
	//  getters & setters
	// ------------------------------------------------------------------------

	/**
	 * Gets the overwrite option.
	 */
	public WriteMode getOverwrite() {
		return overwrite;
	}

	/**
	 * Sets the overwrite option.
	 *
	 * <p>Method returns this object for fluent function call chaining.
	 */
	public WriteOptions setOverwrite(WriteMode overwrite) {
		this.overwrite = checkNotNull(overwrite);
		return this;
	}

	/**
	 * Gets the block writing settings, like size and replication factor.
	 * Returns null if no settings are defined.
	 */
	@Nullable
	public BlockOptions getBlockSettings() {
		return blockSettings;
	}

	/**
	 * Sets the block settings, for file systems working with block replication and
	 * exposing those settings
	 *
	 * <p>Method returns this object for fluent function call chaining.
	 */
	public WriteOptions setBlockSettings(@Nullable BlockOptions blockSettings) {
		this.blockSettings = blockSettings;
		return this;
	}

	/**
	 * Gets whether to inject entropy into the path.
	 */
	public boolean isInjectEntropy() {
		return injectEntropy;
	}

	/**
	 * Sets whether to inject entropy into the path.
	 *
	 * <p>Entropy injection is only supported select filesystems like S3 to overcome
	 * scalability issues in the sharding. For this option to have any effect, the
	 * file system must be configured to replace an entropy key with entropy, and the
	 * path that is written to must contain the entropy key.
	 *
	 * <p>Method returns this object for fluent function call chaining.
	 */
	public WriteOptions setInjectEntropy(boolean injectEntropy) {
		this.injectEntropy = injectEntropy;
		return this;
	}

	// ------------------------------------------------------------------------
	//  nested options classes
	// ------------------------------------------------------------------------

	/**
	 * Settings for block replication. Interpreted only by filesystems that are based
	 * expose block replication settings.
	 */
	@Public
	public static class BlockOptions {

		/** The size of the blocks, in bytes. */
		private long blockSize;

		/** The number of times the block should be replicated. */
		private int replicationFactor;

		public BlockOptions(long blockSize, int replicationFactor) {
			checkArgument(blockSize > 0, "blockSize must be >0");
			checkArgument(replicationFactor > 0, "replicationFactor must be >=1");

			this.blockSize = blockSize;
			this.replicationFactor = replicationFactor;
		}

		/**
		 * Gets the block size, in bytes.
		 */
		public long getBlockSize() {
			return blockSize;
		}

		/**
		 * Gets the number of times the block should be replicated.
		 */
		public int getReplicationFactor() {
			return replicationFactor;
		}
	}
}
