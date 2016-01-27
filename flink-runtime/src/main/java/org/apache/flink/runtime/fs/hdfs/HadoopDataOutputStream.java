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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.flink.core.fs.FSDataOutputStream;

public class HadoopDataOutputStream extends FSDataOutputStream {

	private final org.apache.hadoop.fs.FSDataOutputStream fdos;

	public HadoopDataOutputStream(org.apache.hadoop.fs.FSDataOutputStream fdos) {
		if (fdos == null) {
			throw new NullPointerException();
		}
		this.fdos = fdos;
	}

	@Override
	public void write(int b) throws IOException {
		fdos.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		fdos.write(b, off, len);
	}

	@Override
	public void close() throws IOException {
		fdos.close();
	}

	@Override
	public void flush() throws IOException {
		if (HFLUSH_METHOD != null) {
			try {
				HFLUSH_METHOD.invoke(fdos);
			}
			catch (InvocationTargetException e) {
				Throwable cause = e.getTargetException();
				if (cause instanceof IOException) {
					throw (IOException) cause;
				}
				else if (cause instanceof RuntimeException) {
					throw (RuntimeException) cause;
				}
				else if (cause instanceof Error) {
					throw (Error) cause;
				}
				else {
					throw new IOException("Exception while invoking hflush()", cause);
				}
			}
			catch (IllegalAccessException e) {
				throw new IOException("Cannot invoke hflush()", e);
			}
		}
		else if (HFLUSH_ERROR != null) {
			if (HFLUSH_ERROR instanceof NoSuchMethodException) {
				throw new UnsupportedOperationException("hflush() method is not available in this version of Hadoop.");
			}
			else {
				throw new IOException("Cannot access hflush() method", HFLUSH_ERROR);
			}
		}
		else {
			throw new UnsupportedOperationException("hflush() is not available in this version of Hadoop.");
		}
	}

	@Override
	public void sync() throws IOException {
		if (HSYNC_METHOD != null) {
			try {
				HSYNC_METHOD.invoke(fdos);
			}
			catch (InvocationTargetException e) {
				Throwable cause = e.getTargetException();
				if (cause instanceof IOException) {
					throw (IOException) cause;
				}
				else if (cause instanceof RuntimeException) {
					throw (RuntimeException) cause;
				}
				else if (cause instanceof Error) {
					throw (Error) cause;
				}
				else {
					throw new IOException("Exception while invoking hsync()", cause);
				}
			}
			catch (IllegalAccessException e) {
				throw new IOException("Cannot invoke hsync()", e);
			}
		}
		else if (HSYNC_ERROR != null) {
			if (HSYNC_ERROR instanceof NoSuchMethodException) {
				throw new UnsupportedOperationException("hsync() method is not available in this version of Hadoop.");
			}
			else {
				throw new IOException("Cannot access hsync() method", HSYNC_ERROR);
			}
		}
		else {
			throw new UnsupportedOperationException("hsync() is not available in this version of Hadoop.");
		}
	}

	/**
	 * Gets the wrapped Hadoop output stream.
	 * @return The wrapped Hadoop output stream.
	 */
	public org.apache.hadoop.fs.FSDataOutputStream getHadoopOutputStream() {
		return fdos;
	}
	
	// ------------------------------------------------------------------------
	// utilities to bridge hsync and hflush to hadoop, even through it is not supported in Hadoop 1
	// ------------------------------------------------------------------------
	
	private static final Method HFLUSH_METHOD;
	private static final Method HSYNC_METHOD;
	
	private static final Throwable HFLUSH_ERROR;
	private static final Throwable HSYNC_ERROR;
	
	static {
		Method hflush = null;
		Method hsync = null;

		Throwable flushError = null;
		Throwable syncError = null;
		
		try {
			hflush = org.apache.hadoop.fs.FSDataOutputStream.class.getMethod("hflush");
		}
		catch (Throwable t) {
			flushError = t;
		}

		try {
			hsync = org.apache.hadoop.fs.FSDataOutputStream.class.getMethod("hsync");
		}
		catch (Throwable t) {
			syncError = t;
		}
		
		HFLUSH_METHOD = hflush;
		HSYNC_METHOD = hsync;
		
		HFLUSH_ERROR = flushError;
		HSYNC_ERROR = syncError;
	}
}
