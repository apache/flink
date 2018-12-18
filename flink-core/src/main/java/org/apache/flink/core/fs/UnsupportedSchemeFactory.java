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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A file system factory to throw an UnsupportedFileSystemSchemeException when called.
 */
@Internal
class UnsupportedSchemeFactory implements FileSystemFactory {

	private final String exceptionMessage;

	@Nullable
	private final Throwable exceptionCause;

	public UnsupportedSchemeFactory(String exceptionMessage) {
		this(exceptionMessage, null);
	}

	public UnsupportedSchemeFactory(String exceptionMessage, @Nullable Throwable exceptionCause) {
		this.exceptionMessage = checkNotNull(exceptionMessage);
		this.exceptionCause = exceptionCause;
	}

	@Override
	public String getScheme() {
		return "n/a";
	}

	@Override
	public void configure(Configuration config) {
		// nothing to do here
	}

	@Override
	public FileSystem create(URI fsUri) throws IOException {
		if (exceptionCause == null) {
			throw new UnsupportedFileSystemSchemeException(exceptionMessage);
		}
		else {
			throw new UnsupportedFileSystemSchemeException(exceptionMessage, exceptionCause);
		}
	}
}
