/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.fs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Base class for {@link Writer Writers} that write to a {@link FSDataOutputStream}.
 */
public abstract class StreamWriterBase<T> implements Writer<T> {

	private static Logger LOG = LoggerFactory.getLogger(RollingSink.class);

	/**
	 * The {@code FSDataOutputStream} for the current part file.
	 */
	private transient FSDataOutputStream outStream;

	/**
	 * We use reflection to get the hflush method or use sync as a fallback.
	 * The idea for this and the code comes from the Flume HDFS Sink.
	 */
	private transient Method refHflushOrSync;

	/**
	 * Returns the current output stream, if the stream is open.
	 */
	protected FSDataOutputStream getStream() {
		if (outStream == null) {
			throw new IllegalStateException("Output stream has not been opened");
		}
		return outStream;
	}

	/**
	 * If hflush is available in this version of HDFS, then this method calls
	 * hflush, else it calls sync.
	 * @param os - The stream to flush/sync
	 * @throws java.io.IOException
	 *
	 * <p>
	 * Note: This code comes from Flume
	 */
	protected void hflushOrSync(FSDataOutputStream os) throws IOException {
		try {
			// At this point the refHflushOrSync cannot be null,
			// since register method would have thrown if it was.
			this.refHflushOrSync.invoke(os);
		} catch (InvocationTargetException e) {
			String msg = "Error while trying to hflushOrSync!";
			LOG.error(msg + " " + e.getCause());
			Throwable cause = e.getCause();
			if(cause != null && cause instanceof IOException) {
				throw (IOException)cause;
			}
			throw new RuntimeException(msg, e);
		} catch (Exception e) {
			String msg = "Error while trying to hflushOrSync!";
			LOG.error(msg + " " + e);
			throw new RuntimeException(msg, e);
		}
	}

	/**
	 * Gets the hflush call using reflection. Fallback to sync if hflush is not available.
	 *
	 * <p>
	 * Note: This code comes from Flume
	 */
	private Method reflectHflushOrSync(FSDataOutputStream os) {
		Method m = null;
		if(os != null) {
			Class<?> fsDataOutputStreamClass = os.getClass();
			try {
				m = fsDataOutputStreamClass.getMethod("hflush");
			} catch (NoSuchMethodException ex) {
				LOG.debug("HFlush not found. Will use sync() instead");
				try {
					m = fsDataOutputStreamClass.getMethod("sync");
				} catch (Exception ex1) {
					String msg = "Neither hflush not sync were found. That seems to be " +
							"a problem!";
					LOG.error(msg);
					throw new RuntimeException(msg, ex1);
				}
			}
		}
		return m;
	}

	@Override
	public void open(FileSystem fs, Path path) throws IOException {
		if (outStream != null) {
			throw new IllegalStateException("Writer has already been opened");
		}
		outStream = fs.create(path, false);
		if (refHflushOrSync == null) {
			refHflushOrSync = reflectHflushOrSync(outStream);
		}
	}

	@Override
	public long flush() throws IOException {
		if (outStream == null) {
			throw new IllegalStateException("Writer is not open");
		}
		hflushOrSync(outStream);
		return outStream.getPos();
	}

	@Override
	public long getPos() throws IOException {
		if (outStream == null) {
			throw new IllegalStateException("Writer is not open");
		}
		return outStream.getPos();
	}

	@Override
	public void close() throws IOException {
		if (outStream != null) {
			flush();
			outStream.close();
			outStream = null;
		}
	}

}
