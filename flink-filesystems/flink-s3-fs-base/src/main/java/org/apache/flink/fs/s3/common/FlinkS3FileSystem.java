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

package org.apache.flink.fs.s3.common;

import org.apache.flink.core.fs.EntropyInjectingFileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Implementation of the Flink {@link org.apache.flink.core.fs.FileSystem} interface for S3.
 * This class implements the common behavior implemented directly by Flink and delegates
 * common calls to an implementation of Hadoop's filesystem abstraction.
 */
public class FlinkS3FileSystem extends HadoopFileSystem implements EntropyInjectingFileSystem {

	@Nullable
	private final String entropyInjectionKey;

	private final int entropyLength;

	/**
	 * Creates a FlinkS3FileSystem based on the given Hadoop S3 file system.
	 * The given Hadoop file system object is expected to be initialized already.
	 *
	 * @param hadoopS3FileSystem The Hadoop FileSystem that will be used under the hood.
	 */
	public FlinkS3FileSystem(org.apache.hadoop.fs.FileSystem hadoopS3FileSystem) {
		this(hadoopS3FileSystem, null, -1);
	}

	/**
	 * Creates a FlinkS3FileSystem based on the given Hadoop S3 file system.
	 * The given Hadoop file system object is expected to be initialized already.
	 *
	 * <p>This constructor additionally configures the entropy injection for the file system.
	 *
	 * @param hadoopS3FileSystem The Hadoop FileSystem that will be used under the hood.
	 * @param entropyInjectionKey The substring that will be replaced by entropy or removed.
	 * @param entropyLength The number of random alphanumeric characters to inject as entropy.
	 */
	public FlinkS3FileSystem(
			org.apache.hadoop.fs.FileSystem hadoopS3FileSystem,
			@Nullable String entropyInjectionKey,
			int entropyLength) {

		super(hadoopS3FileSystem);

		if (entropyInjectionKey != null && entropyLength <= 0) {
			throw new IllegalArgumentException("Entropy length must be >= 0 when entropy injection key is set");
		}

		this.entropyInjectionKey = entropyInjectionKey;
		this.entropyLength = entropyLength;
	}

	// ------------------------------------------------------------------------

	@Nullable
	@Override
	public String getEntropyInjectionKey() {
		return entropyInjectionKey;
	}

	@Override
	public String generateEntropy() {
		return StringUtils.generateRandomAlphanumericString(ThreadLocalRandom.current(), entropyLength);
	}

	@Override
	public FileSystemKind getKind() {
		return FileSystemKind.OBJECT_STORE;
	}
}
