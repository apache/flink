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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileSystemKindTest {

	@Test
	public void testPosixFileSystemKind() {
		final FileSystemKind kind = FileSystemKind.POSIX_COMPLIANT;
		assertEquals(ConsistencyLevel.POSIX_STYLE_CONSISTENCY, kind.consistencyLevel());

		assertTrue(kind.supportsDeleteOfOpenFiles());
		assertTrue(kind.supportsRealDirectories());
		assertTrue(kind.supportsEfficientRecursiveDeletes());
	}

	@Test
	public void testConsistentFileSystemKind() {
		final FileSystemKind kind = FileSystemKind.CONSISTENT_FILESYSTEM;

		assertEquals(ConsistencyLevel.CONSISTENT_LIST_RENAME_DELETE, kind.consistencyLevel());
		assertFalse(kind.supportsDeleteOfOpenFiles());
		assertTrue(kind.supportsRealDirectories());
		assertTrue(kind.supportsEfficientRecursiveDeletes());
	}

	@Test
	public void testConsistentObjectStoreKind() {
		final FileSystemKind kind = FileSystemKind.CONSISTENT_OBJECT_STORE;

		assertEquals(ConsistencyLevel.CONSISTENT_RENAME_DELETE, kind.consistencyLevel());
		assertFalse(kind.supportsDeleteOfOpenFiles());
		assertFalse(kind.supportsRealDirectories());
		assertFalse(kind.supportsEfficientRecursiveDeletes());
	}

	@Test
	public void testEventuallyConsistentObjectStoreKind() {
		final FileSystemKind kind = FileSystemKind.EVENTUALLY_CONSISTENT_OBJECT_STORE;

		assertEquals(ConsistencyLevel.READ_AFTER_CREATE, kind.consistencyLevel());
		assertFalse(kind.supportsDeleteOfOpenFiles());
		assertFalse(kind.supportsRealDirectories());
		assertFalse(kind.supportsEfficientRecursiveDeletes());
	}
}
