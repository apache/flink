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

package org.apache.flink.fs.s3.common.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link FSDataOutputStream} with the {@link RefCounted} functionality.
 */
@Internal
public abstract class RefCountedFSOutputStream extends FSDataOutputStream implements RefCounted {

	/**
	 * Gets an {@link InputStream} that allows to read the contents of the file.
	 *
	 * @return An input stream to the contents of the file.
	 */
	public abstract InputStream getInputStream() throws IOException;

	/**
	 * Checks if the file is closed for writes.
	 *
	 * @return {@link true} if the file is closed, {@link false} otherwise.
	 */
	public abstract boolean isClosed() throws IOException;
}
