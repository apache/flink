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

package org.apache.flink.runtime.io.disk;

import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel.Enumerator;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel.ID;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The manager used for creating/deleting file channels based on config temp dirs.
 */
public class FileChannelManagerImpl implements FileChannelManager {
	private static final Logger LOG = LoggerFactory.getLogger(FileChannelManagerImpl.class);

	/** The temporary directories for files. */
	private final File[] paths;

	/** A random number generator for the anonymous Channel IDs. */
	private final Random random;

	/** The number of the next path to use. */
	private volatile int nextPath;

	public FileChannelManagerImpl(String[] tempDirs, String prefix) {
		checkNotNull(tempDirs, "The temporary directories must not be null.");
		checkArgument(tempDirs.length > 0, "The temporary directories must not be empty.");

		this.random = new Random();
		this.nextPath = 0;
		this.paths = createFiles(tempDirs, prefix);
	}

	private static File[] createFiles(String[] tempDirs, String prefix) {
		File[] files = new File[tempDirs.length];
		for (int i = 0; i < tempDirs.length; i++) {
			File baseDir = new File(tempDirs[i]);
			String subfolder = String.format("flink-%s-%s", prefix, UUID.randomUUID().toString());
			File storageDir = new File(baseDir, subfolder);

			if (!storageDir.exists() && !storageDir.mkdirs()) {
				throw new RuntimeException(
					"Could not create storage directory for FileChannelManager: " + storageDir.getAbsolutePath());
			}
			files[i] = storageDir;

			LOG.info("FileChannelManager uses directory {} for spill files.", storageDir.getAbsolutePath());
		}
		return files;
	}

	@Override
	public ID createChannel() {
		int num = getNextPathNum();
		return new ID(paths[num], num, random);
	}

	@Override
	public Enumerator createChannelEnumerator() {
		return new Enumerator(paths, random);
	}

	@Override
	public File[] getPaths() {
		return Arrays.copyOf(paths, paths.length);
	}

	/**
	 * Remove all the temp directories.
	 */
	@Override
	public void close() throws Exception {
		IOUtils.closeAll(Arrays.stream(paths)
			.filter(File::exists)
			.map(FileChannelManagerImpl::getFileCloser)
			.collect(Collectors.toList()));
	}

	private static AutoCloseable getFileCloser(File path) {
		return () -> {
			try {
				FileUtils.deleteDirectory(path);
				LOG.info("FileChannelManager removed spill file directory {}", path.getAbsolutePath());
			} catch (IOException e) {
				String errorMessage = String.format("FileChannelManager failed to properly clean up temp file directory: %s", path);
				throw new IOException(errorMessage, e);
			}
		};
	}

	private int getNextPathNum() {
		int next = nextPath;
		int newNext = next + 1;
		nextPath = newNext >= paths.length ? 0 : newNext;
		return next;
	}
}
