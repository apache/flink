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

import java.io.IOException;

/**
 * An exception to indicate that a specific file system scheme is not supported.
 */
public class UnsupportedFileSystemSchemeException extends IOException {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new exception with the given message.
	 *
	 * @param message The exception message
	 */
	public UnsupportedFileSystemSchemeException(String message) {
		super(message);
	}

	/**
	 * Creates a new exception with the given message and cause.
	 *
	 * @param message The exception message
	 * @param cause The exception cause
	 */
	public UnsupportedFileSystemSchemeException(String message, Throwable cause) {
		super(message, cause);
	}
}
