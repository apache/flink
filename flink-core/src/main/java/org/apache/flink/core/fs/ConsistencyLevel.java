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
 * An enumeration describing the level of consistency offered by a {@link FileSystem}.
 * 
 * <p>The consistency levels described here make statements about the visibility
 * of file existence and file contents in the presence of <i>new file creation</i>,
 * <i>file deletion</i>, <i>file renaming</i>, and <i>directory listing</i>.
 * 
 * <p>An operation is defined as consistent if the following holds: After the function
 * call triggering the operation returns, its result is immediately reflected in
 * in the view presented to any other party calling a file system function.
 * 
 * <p>Please note that these levels do not make any statements about the effects or visibility of
 * file content modification or file appends. In fact, content modification or appending are
 * not supported in various file systems.
 * 
 * <p>Some of these consistency levels indicate that the storage system does not actually
 * qualify to be called a FileSystem, but rather a blob-/object store.
 */
@PublicEvolving
public enum ConsistencyLevel {

	/**
	 * This consistency level only guarantees that files are visible with a consistent
	 * view of their contents after their initial creation, once the writing stream has been closed.
	 * Any modifications, renames, deletes are not guaranteed to be immediately visible in a
	 * consistent manner.
	 * 
	 * <p>To access a file/object consistently, the full path/key must be provided. Enumeration
	 * of files/objects is not consistent.
	 * 
	 * <p>An example of a storage system with this consistency level is Amazon's S3 object store.
	 * 
	 * <p>This is the weakest consistency level with which Flink's checkpointing can work.
	 * 
	 * <b>Summary:</b>
	 * <ul>
	 *     <li>New file creation: Consistent
	 *     <li>File deletion: NOT consistent
	 *     <li>File renaming: NOT consistent
	 *     <li>Directory listing: NOT consistent
	 * </ul>
	 */
	READ_AFTER_CREATE,

	/**
	 * This consistency level guarantees that files are visible with a consistent
	 * view of their contents after their initial creation, and after renaming them.
	 * The non-existence is consistently visible after delete operations.
	 *
	 * <p>To access a file/object consistently, the full path/key must be provided. Enumeration
	 * of files/objects is not necessarily consistent.
	 *
	 * <b>Summary:</b>
	 * <ul>
	 *     <li>New file creation: consistent
	 *     <li>File deletion: consistent
	 *     <li>File renaming: consistent, but not necessarily atomic
	 *     <li>Directory listing: NOT consistent
	 * </ul>
	 */
	CONSISTENT_RENAME_DELETE,
	
	/**
	 * This consistency level guarantees that files are visible with a consistent
	 * view of their contents after their initial creation, as well as after renaming operations.
	 * File deletion is immediately visible to all parties.
	 * 
	 * <p>Directory listings are consistent, meaning after file creation/rename/delete, the file
	 * existence or non-existence is reflected when enumerating the parent directory's contents.
	 *
	 * <p>An example of storage systems and file systems falling under this consistency level are
	 * HDFS, MapR FS, and the Windows file systems.
	 * 
	 * <b>Summary:</b>
	 * <ul>
	 *     <li>New file creation: consistent
	 *     <li>File deletion: consistent
	 *     <li>File renaming: consistent, but not necessarily atomic
	 *     <li>Directory listing: consistent
	 * </ul>
	 */
	CONSISTENT_LIST_RENAME_DELETE,

	/**
	 * This consistency level has all guarantees of {@link ConsistencyLevel#CONSISTENT_LIST_RENAME_DELETE}
	 * and supports in addition atomic renames of files.
	 * 
	 * <b>Summary:</b>
	 * <ul>
	 *     <li>New file creation: consistent
	 *     <li>File deletion: consistent
	 *     <li>File renaming: consistent and atomic
	 *     <li>Directory listing: consistent
	 * </ul>
	 */
	POSIX_STYLE_CONSISTENCY
}
