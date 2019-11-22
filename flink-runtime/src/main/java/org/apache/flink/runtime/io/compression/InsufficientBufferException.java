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

package org.apache.flink.runtime.io.compression;

/**
 * An {@code InsufficientBufferException} is thrown when there is no enough buffer to
 * serialize or deserialize a buffer to another buffer. When such exception being caught,
 * user may enlarge the output buffer and try again.
 */
public class InsufficientBufferException extends RuntimeException {

	public InsufficientBufferException() {
		super();
	}

	public InsufficientBufferException(String message) {
		super(message);
	}

	public InsufficientBufferException(String message, Throwable e) {
		super(message, e);
	}

	public InsufficientBufferException(Throwable e) {
		super(e);
	}
}
