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

package org.apache.flink.runtime.fs.hdfs.truncate;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;

/**
 * Concrete implementation of the {@link Truncater} which support truncate logic for Hadoop 2.7 and higher.
 */
class NativeTruncater implements Truncater {

	private static final long LEASE_TIMEOUT = 100_000L;

	private Method truncateHandle;

	public NativeTruncater() {
		ensureTruncateInitialized();
	}

	@Override
	public void truncate(FileSystem hadoopFs, Path path, long length) throws IOException {
		waitUntilLeaseIsRevoked(hadoopFs, path);

		// truncate back and append
		boolean truncated;
		try {
			truncated = invokeTruncate(hadoopFs, path, length);
		} catch (Exception e) {
			throw new IOException("Problem while truncating file: " + path, e);
		}

		if (!truncated) {
			// Truncate did not complete immediately, we must wait for
			// the operation to complete and release the lease.
			waitUntilLeaseIsRevoked(hadoopFs, path);
		}
	}

	private boolean invokeTruncate(FileSystem hadoopFs, Path file, long length) throws IOException {
		if (truncateHandle != null) {
			try {
				return (Boolean) truncateHandle.invoke(hadoopFs, file, length);
			} catch (InvocationTargetException e) {
				ExceptionUtils.rethrowIOException(e.getTargetException());
			} catch (Throwable t) {
				throw new IOException(
					"Truncation of file failed because of access/linking problems with Hadoop's truncate call. " +
						"This is most likely a dependency conflict or class loading problem.");
			}
		} else {
			throw new IllegalStateException("Truncation handle has not been initialized");
		}
		return false;
	}

	/**
	 * Called when resuming execution after a failure and waits until the lease
	 * of the file we are resuming is free.
	 *
	 * <p>The lease of the file we are resuming writing/committing to may still
	 * belong to the process that failed previously and whose state we are
	 * recovering.
	 *
	 * @param path The path to the file we want to resume writing to.
	 */
	private static boolean waitUntilLeaseIsRevoked(final FileSystem fs, final Path path) throws IOException {
		Preconditions.checkState(fs instanceof DistributedFileSystem);

		final DistributedFileSystem dfs = (DistributedFileSystem) fs;
		dfs.recoverLease(path);

		final Deadline deadline = Deadline.now().plus(Duration.ofMillis(LEASE_TIMEOUT));

		final StopWatch sw = new StopWatch();
		sw.start();

		boolean isClosed = dfs.isFileClosed(path);
		while (!isClosed && deadline.hasTimeLeft()) {
			try {
				Thread.sleep(500L);
			} catch (InterruptedException e1) {
				throw new IOException("Recovering the lease failed: ", e1);
			}
			isClosed = dfs.isFileClosed(path);
		}
		return isClosed;
	}

	// ------------------------------------------------------------------------
	//  Reflection utils for truncation
	//    These are needed to compile against Hadoop versions before
	//    Hadoop 2.7, which have no truncation calls for HDFS.
	// ------------------------------------------------------------------------

	private void ensureTruncateInitialized() throws FlinkRuntimeException {
		if (truncateHandle == null) {
			Method truncateMethod;
			try {
				truncateMethod = FileSystem.class.getMethod("truncate", Path.class, long.class);
			} catch (NoSuchMethodException e) {
				throw new FlinkRuntimeException("Could not find a public truncate method on the Hadoop File System.");
			}

			if (!Modifier.isPublic(truncateMethod.getModifiers())) {
				throw new FlinkRuntimeException("Could not find a public truncate method on the Hadoop File System.");
			}

			truncateHandle = truncateMethod;
		}
	}

}
