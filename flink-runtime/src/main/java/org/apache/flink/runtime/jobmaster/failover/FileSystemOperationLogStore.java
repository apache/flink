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

package org.apache.flink.runtime.jobmaster.failover;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * Implementation of {@link OperationLogStore} that store all {@link OperationLog}
 * on a {@link FileSystem}.
 */
public class FileSystemOperationLogStore implements OperationLogStore {

	private static final Logger LOG = LoggerFactory.getLogger(FileSystemOperationLogStore.class);

	private static final String logNamePrefix = "operation.log.";

	private final Path workingDir;

	private FileSystem fileSystem;

	private Path filePath;

	private DataOutputStream outputStream;

	private FileFlusher fileFlusher;

	private FileSystemOperationLogReader opLogReader;

	private volatile boolean currupted = false;

	private final int flushIntervalInMs;

	//------------------------- Constructor for testing -------------------------

	@VisibleForTesting
	FileSystemOperationLogStore (@Nonnull Path workingDir) {
		this.workingDir = workingDir;
		this.flushIntervalInMs = 1000;
		try {
			this.fileSystem = workingDir.getFileSystem();
		} catch (Exception e) {
			throw new FlinkRuntimeException("Fail to get file system.");
		}
	}

	//---------------------------------------------------------------------------

	/**
	 * Instantiates a new {@link FileSystemOperationLogStore}.
	 *
	 * @param jobID         the job id
	 * @param configuration the configuration
	 */
	public FileSystemOperationLogStore(
		@Nonnull JobID jobID,
		@Nonnull Configuration configuration
	) {
		// build working dir
		String rootPath = configuration.getValue(HighAvailabilityOptions.HA_STORAGE_PATH);
		if (rootPath == null || StringUtils.isBlank(rootPath)) {
			throw new IllegalConfigurationException(
				String.format("Missing high-availability storage path for storing operation logs." +
					" Specify via configuration key '%s'.", HighAvailabilityOptions.HA_STORAGE_PATH));
		}

		rootPath += configuration.getString(HighAvailabilityOptions.HA_CLUSTER_ID);

		this.workingDir = new Path(new Path(rootPath, jobID.toString()), "jobmaster-oplog");
		this.flushIntervalInMs = configuration.getInteger(JobManagerOptions.OPLOG_FLUSH_INTERVAL);
		try {
			this.fileSystem = workingDir.getFileSystem();
		} catch (Exception e) {
			throw new FlinkRuntimeException("Fail to get file system.");
		}
	}

	/**
	 * Set the {@link OperationLogStore} to be ready to work. This method should
	 * be called before any other operates.
	 */
	@Override
	public void start() {
		try {
			if (!fileSystem.exists(workingDir)) {
				fileSystem.mkdirs(workingDir);
			}

			for (int i = 1; ; i++) {
				Path file = new Path(workingDir, logNamePrefix + i);

				if (!fileSystem.exists(file)) {
					outputStream = new DataOutputStream(fileSystem.create(file, WriteMode.NO_OVERWRITE));
					filePath = file;
					assert !fileSystem.exists(new Path(workingDir, logNamePrefix + (i + 1)));
					LOG.info("Operation log will be written to {}.", filePath);
					break;
				}
			}
			fileFlusher = new FileFlusher();
			fileFlusher.start();
		} catch (Exception e) {
			throw new FlinkRuntimeException("Fail to start file system log store.", e);
		}
	}

	/**
	 * Stop writing readOpLog.
	 */
	@Override
	public void stop() {
		try {
			if (outputStream != null) {
				outputStream.flush();
				outputStream.close();
				outputStream = null;
			}
			if (fileFlusher != null) {
				fileFlusher.interrupt();
				fileFlusher.join();
				fileFlusher = null;
			}
			if (opLogReader != null) {
				opLogReader.close();
				opLogReader = null;
			}
		} catch (Exception e) {
			LOG.warn("Fail to stop {}.", filePath.getName(), e);
		}
	}

	/**
	 * Clear readOpLog written so far by delete the file. {@link OperationLogStore}
	 * reaches a terminate state after be clear(). Call start() before any other
	 * operates.
	 */
	@Override
	public void clear() {
		try {
			if (outputStream != null) {
				outputStream.close();
				outputStream = null;
			}
			if (fileFlusher != null) {
				fileFlusher.interrupt();
				fileFlusher.join();
			}
			if (opLogReader != null) {
				opLogReader.close();
				opLogReader = null;
			}

			fileSystem.delete(workingDir, true);
		} catch (Exception e) {
			LOG.warn("Fail to delete the {}.", workingDir.getName(), e);
		}
	}

	@Override
	public void writeOpLog(@Nonnull OperationLog opLog) {
		// outputStream would be null if you start() to write, then stop(),
		// and then re-start() to write. This is because we do not assume all
		// filesystems support append an existing file.
		// In this case, you should clear() the opLog store first, and then
		// re-start() to get a new opLog store.
		Preconditions.checkNotNull(outputStream);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Writing a operation log on file system at {} while {}.", filePath, currupted);
		}

		try {
			if (!currupted) {
				byte[] bytes = InstantiationUtil.serializeObject(opLog);
				outputStream.writeInt(bytes.length);
				outputStream.write(bytes);
			}
		} catch (Exception e) {
			LOG.warn("Write log meet error, will not record log any more.", e);
			currupted = true;
		}
	}

	@Override
	public OperationLog readOpLog() {
		if (opLogReader == null) {
			opLogReader = new FileSystemOperationLogReader();
		}
		return opLogReader.read();
	}

	class FileSystemOperationLogReader {

		private DataInputStream inputStream;

		private int index = 1;

		FileSystemOperationLogReader() {
			try {
				Path file = new Path(workingDir, logNamePrefix + index++);
				inputStream = new DataInputStream(fileSystem.open(file));
			} catch (IOException e) {
				throw new FlinkRuntimeException("Cannot init filesystem opLog store.", e);
			}
		}

		public OperationLog read() {
			try {
				OperationLog operationLog = null;
				while (operationLog == null) {
					try {
						int logLength = inputStream.readInt();
						byte[] logByte = new byte[logLength];
						int logReadLen = inputStream.read(logByte);
						if (logReadLen == logLength) {
							operationLog = InstantiationUtil.deserializeObject(logByte, ClassLoader.getSystemClassLoader());
						} else {
							String message = String.format("Fail to read log from %s%s, expected %, only read %s",
									logNamePrefix, index, logLength, logReadLen);
							throw new IOException(message);
						}
					} catch (EOFException eof) {
						Path file = new Path(workingDir, logNamePrefix + index++);
						if (fileSystem.exists(file)) {
							inputStream.close();
							inputStream = new DataInputStream(fileSystem.open(file));
						} else {
							break;
						}
					}
				}
				return operationLog == null ? null : operationLog;
			} catch (Exception e) {
				throw new FlinkRuntimeException("Cannot read next opLog from opLog store.", e);
			}
		}

		public void close() throws IOException {
			if (inputStream != null) {
				inputStream.close();
				inputStream = null;
			}
		}
	}

	private class FileFlusher extends Thread {

		@Override
		public void run() {
			while (outputStream != null && !currupted) {
				try {
					outputStream.flush();
					Thread.sleep(flushIntervalInMs);
				} catch (Exception e) {
					LOG.warn("Fail to flush the log file!", e);
				}
			}
		}
	}
}
