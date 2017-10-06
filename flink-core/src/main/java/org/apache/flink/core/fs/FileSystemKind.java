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

import org.apache.flink.annotation.PublicEvolving;

/**
 * An enumeration defining the kind and characteristics of a {@link FileSystem}.
 * 
 * <p>Note that the categorization here makes statements only about <i>consistency</i> and
 * <i>directory handling</i>. It explicitly does not look at the ability to modify files,
 * which we assume for no operation going through Flink's File System abstraction, at the moment.
 * This might change in the future. 
 */
@PublicEvolving
public enum FileSystemKind {

	/**
	 * A POSIX compliant file system, as for example found on UNIX / Linux.
	 * 
	 * <p>Posix file systems support directories, a consistent view, atomic renames,
	 * and deletion of open files.
	 */
	POSIX_COMPLIANT(ConsistencyLevel.POSIX_STYLE_CONSISTENCY),

	/**
	 * A file system that gives a consistent view of its contents.
	 * 
	 * <p>File systems in this category are for example Windows file systems,
	 * HDFS, or MapR FS. They support directories, a consistent view, but not
	 * necessarily atomic file renaming, or deletion of open files.
	 */
	CONSISTENT_FILESYSTEM(ConsistencyLevel.CONSISTENT_LIST_RENAME_DELETE),

	/**
	 * A consistent object store (not an actual file system).
	 *
	 * <p>"File systems" of this kind support no real directories, but  and no consistent
	 * renaming and delete operations.
	 */
	CONSISTENT_OBJECT_STORE(ConsistencyLevel.CONSISTENT_RENAME_DELETE),

	/**
	 * An eventually consistent object store (not an actual file system), like
	 * Amazon's S3.
	 * 
	 * <p>"File systems" of this kind support no real directories and no consistent
	 * renaming and delete operations.
	 */
	EVENTUALLY_CONSISTENT_OBJECT_STORE(ConsistencyLevel.READ_AFTER_CREATE);

	// ------------------------------------------------------------------------

	private final ConsistencyLevel consistencyLevel;

	FileSystemKind(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}

	/**
	 * Gets the consistency level of the file system, which describes how consistently
	 * visible operations like file creation, deletion, and directory listings are.
	 */
	public ConsistencyLevel consistencyLevel() {
		return consistencyLevel;
	}


	/**
	 * Checks whether the file system support real directories, or whether it actually only
	 * encodes a hierarchy into the file names. 
	 */
	public boolean supportsRealDirectories() {
		return this != EVENTUALLY_CONSISTENT_OBJECT_STORE && this != CONSISTENT_OBJECT_STORE;
	}

	/**
	 * Checks whether the file system supports efficient recursive directory deletes, or
	 * whether the
	 */
	public boolean supportsEfficientRecursiveDeletes() {
		return this != EVENTUALLY_CONSISTENT_OBJECT_STORE && this != CONSISTENT_OBJECT_STORE;
	}

	/**
	 * Checks whether the file system supports to delete files from a directory, while there
	 * are still open streams to the file.
	 *
	 * <p>File systems that do not support that operation typically throw an exception when trying
	 * to delete that file.
	 */
	public boolean supportsDeleteOfOpenFiles() {
		return this == POSIX_COMPLIANT;
	}
}
