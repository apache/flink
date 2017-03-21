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

/**
 * Simple container to hold an exception and the corresponding timestamp.
 */
class ErrorInfo {
	private volatile Throwable exception;
	private volatile long timestamp;

	/**
	 * Sets the exception and corresponding timestamp.
	 * 
	 * @param exception Exception to set
	 * @param timestamp Timestamp when exception occurred
	 */
	void setExceptionAndTimestamp(Throwable exception, long timestamp) {
		this.exception = exception;
		this.timestamp = timestamp;
	}

	/**
	 * Returns the contained exception.
	 *
	 * @return contained exception, or null if no exception was set yet
	 */
	Throwable getException() {
		return exception;
	}

	/**
	 * Returns the timestamp for the contained exception.
	 *
	 * @return timestamp of contained exception, or 0 if no exception was set yet
	 */
	long getTimestamp() {
		return timestamp;
	}
}
