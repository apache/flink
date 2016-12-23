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

package org.apache.flink.core.fs.local;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FileSystem;

/**
 * The class <code>LocalDistributedFileSystem</code> provides an implementation
 * of the {@link FileSystem} interface for a locally mounted distributed file
 * system, e.g. NFS.
 */
@Internal
public class LocalDistributedFileSystem extends LocalFileSystem {
	/**
	 * Constructs a new <code>LocalDistributedFileSystem</code> object.
	 */
	public LocalDistributedFileSystem() {
		super();
	}

	@Override
	public boolean isDistributedFS() {
		return true;
	}
}
