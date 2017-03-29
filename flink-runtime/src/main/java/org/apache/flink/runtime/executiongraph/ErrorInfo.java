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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Simple container to hold an exception and the corresponding timestamp.
 */
public class ErrorInfo implements Serializable {

	private static final long serialVersionUID = -6138942031953594202L;

	private final transient Throwable exception;
	private final long timestamp;

	private volatile String exceptionAsString;

	public ErrorInfo(Throwable exception, long timestamp) {
		Preconditions.checkNotNull(exception);
		Preconditions.checkArgument(timestamp > 0);

		this.exception = exception;
		this.timestamp = timestamp;
	}

	/**
	 * Returns the contained exception.
	 *
	 * @return contained exception, or {@code "(null)"} if either no exception was set or this object has been deserialized
	 */
	Throwable getException() {
		return exception;
	}

	/**
	 * Returns the contained exception as a string.
	 *
	 * @return failure causing exception as a string, or {@code "(null)"}
	 */
	public String getExceptionAsString() {
		if (exceptionAsString == null) {
			exceptionAsString = ExceptionUtils.stringifyException(exception);
		}
		return exceptionAsString;
	}

	/**
	 * Returns the timestamp for the contained exception.
	 *
	 * @return timestamp of contained exception, or 0 if no exception was set
	 */
	public long getTimestamp() {
		return timestamp;
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		// make sure that the exception was stringified so it isn't lost during serialization
		if (exceptionAsString == null) {
			exceptionAsString = ExceptionUtils.stringifyException(exception);
		}
		out.defaultWriteObject();
	}
}
