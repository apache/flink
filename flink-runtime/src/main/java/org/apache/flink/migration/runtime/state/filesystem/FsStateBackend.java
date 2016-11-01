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

package org.apache.flink.migration.runtime.state.filesystem;

import org.apache.flink.core.fs.Path;
import org.apache.flink.migration.runtime.state.AbstractStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class FsStateBackend extends AbstractStateBackend {

	private static final long serialVersionUID = -8191916350224044011L;

	private static final Logger LOG = LoggerFactory.getLogger(FsStateBackend.class);

	/** By default, state smaller than 1024 bytes will not be written to files, but
	 * will be stored directly with the metadata */
	public static final int DEFAULT_FILE_STATE_THRESHOLD = 1024;

	/** Maximum size of state that is stored with the metadata, rather than in files */
	public static final int MAX_FILE_STATE_THRESHOLD = 1024 * 1024;
	
	/** Default size for the write buffer */
	private static final int DEFAULT_WRITE_BUFFER_SIZE = 4096;
	

	/** The path to the directory for the checkpoint data, including the file system
	 * description via scheme and optional authority */
	private final Path basePath = null;

	/** State below this size will be stored as part of the metadata, rather than in files */
	private final int fileStateThreshold = 0;
}
