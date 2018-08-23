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

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Concrete implementation of the {@link TruncateManager} which support truncate logic for Hadoop 2.7 and higher.
 */
public class NativeTruncateManager implements TruncateManager {

	private Method truncateHandle;

	private FileSystem hadoopFs;

	public NativeTruncateManager(FileSystem hadoopFs) {
		ensureTruncateInitialized();
		this.hadoopFs = hadoopFs;
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

	@Override
	public void truncate(Path file, long length) throws IOException {
		if (truncateHandle != null) {
			try {
				truncateHandle.invoke(hadoopFs, file, length);
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
	}

}
