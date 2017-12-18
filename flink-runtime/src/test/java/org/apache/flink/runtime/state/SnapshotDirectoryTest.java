/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

public class SnapshotDirectoryTest {

	private static TemporaryFolder temporaryFolder;

	@BeforeClass
	public static void beforeClass() throws IOException {
		temporaryFolder = new TemporaryFolder();
		temporaryFolder.create();
	}

	@AfterClass
	public static void afterClass() {
		temporaryFolder.delete();
	}

	/**
	 * Tests if mkdirs for snapshot directories works.
	 */
	@Test
	public void mkdirs() throws Exception {
		File folderRoot = temporaryFolder.getRoot();
		File newFolder = new File(folderRoot, String.valueOf(UUID.randomUUID()));
		File innerNewFolder = new File(newFolder, String.valueOf(UUID.randomUUID()));
		Path path = new Path(innerNewFolder.toURI());

		Assert.assertFalse(newFolder.isDirectory());
		Assert.assertFalse(innerNewFolder.isDirectory());
		SnapshotDirectory snapshotDirectory = new SnapshotDirectory(path);
		Assert.assertFalse(snapshotDirectory.exists());
		Assert.assertFalse(newFolder.isDirectory());
		Assert.assertFalse(innerNewFolder.isDirectory());

		Assert.assertTrue(snapshotDirectory.mkdirs());
		Assert.assertTrue(newFolder.isDirectory());
		Assert.assertTrue(innerNewFolder.isDirectory());
		Assert.assertTrue(snapshotDirectory.exists());
	}

	/**
	 * Tests if indication of directory existence works.
	 */
	@Test
	public void exists() throws Exception {
		File folderRoot = temporaryFolder.getRoot();
		File folderA = new File(folderRoot, String.valueOf(UUID.randomUUID()));

		Assert.assertFalse(folderA.isDirectory());
		SnapshotDirectory snapshotDirectory = new SnapshotDirectory(new Path(folderA.toURI()));
		Assert.assertFalse(snapshotDirectory.exists());
		Assert.assertTrue(folderA.mkdirs());
		Assert.assertTrue(snapshotDirectory.exists());
		Assert.assertTrue(folderA.delete());
		Assert.assertFalse(snapshotDirectory.exists());
	}

	/**
	 * Tests listing of file statuses works like listing on the path directly.
	 */
	@Test
	public void listStatus() throws Exception {
		File folderRoot = temporaryFolder.getRoot();
		File folderA = new File(folderRoot, String.valueOf(UUID.randomUUID()));
		File folderB = new File(folderA, String.valueOf(UUID.randomUUID()));
		Assert.assertTrue(folderB.mkdirs());
		File file = new File(folderA, "test.txt");
		Assert.assertTrue(file.createNewFile());

		Path path = new Path(folderA.toURI());
		FileSystem fileSystem = path.getFileSystem();
		SnapshotDirectory snapshotDirectory = new SnapshotDirectory(path);
		Assert.assertTrue(snapshotDirectory.exists());

		Assert.assertEquals(
			Arrays.toString(fileSystem.listStatus(path)),
			Arrays.toString(snapshotDirectory.listStatus()));
	}

	/**
	 * Tests that reporting the handle of a completed snapshot works as expected and that the directory for completed
	 * snapshot is not deleted by {@link #deleteIfIncompleteSnapshot()}.
	 */
	@Test
	public void completeSnapshotAndGetHandle() throws Exception {
		File folderRoot = temporaryFolder.getRoot();
		File folderA = new File(folderRoot, String.valueOf(UUID.randomUUID()));
		Assert.assertTrue(folderA.mkdirs());
		Path folderAPath = new Path(folderA.toURI());
		SnapshotDirectory snapshotDirectory = new SnapshotDirectory(folderAPath);

		// check that completed checkpoint dirs are not deleted as incomplete.
		DirectoryStateHandle handle = snapshotDirectory.completeSnapshotAndGetHandle();
		Assert.assertFalse(snapshotDirectory.deleteIfIncompleteSnapshot());
		Assert.assertTrue(folderA.isDirectory());
		Assert.assertEquals(folderAPath, handle.getDirectory());
		handle.discardState();

		Assert.assertFalse(folderA.isDirectory());
		Assert.assertTrue(folderA.mkdirs());
		snapshotDirectory = new SnapshotDirectory(folderAPath);
		Assert.assertTrue(snapshotDirectory.deleteIfIncompleteSnapshot());
		try {
			snapshotDirectory.completeSnapshotAndGetHandle();
			Assert.fail();
		} catch (IOException ignore) {
		}
	}

	/**
	 * Tests that snapshot director behaves correct for delete calls. Completed snapshots should not be deleted,
	 * only ongoing snapshots can.
	 */
	@Test
	public void deleteIfIncompleteSnapshot() throws Exception {
		File folderRoot = temporaryFolder.getRoot();
		File folderA = new File(folderRoot, String.valueOf(UUID.randomUUID()));
		File folderB = new File(folderA, String.valueOf(UUID.randomUUID()));
		Assert.assertTrue(folderB.mkdirs());
		File file = new File(folderA, "test.txt");
		Assert.assertTrue(file.createNewFile());
		Path folderAPath = new Path(folderA.toURI());
		SnapshotDirectory snapshotDirectory = new SnapshotDirectory(folderAPath);
		Assert.assertTrue(snapshotDirectory.deleteIfIncompleteSnapshot());
		Assert.assertFalse(folderA.isDirectory());
		Assert.assertTrue(folderA.mkdirs());
		snapshotDirectory = new SnapshotDirectory(folderAPath);
		snapshotDirectory.completeSnapshotAndGetHandle();
		Assert.assertFalse(snapshotDirectory.deleteIfIncompleteSnapshot());
		Assert.assertTrue(folderA.isDirectory());
	}

	/**
	 * This test checks that completing or deleting the snapshot influence the #isSnapshotOngoing() flag.
	 */
	@Test
	public void isSnapshotOngoing() throws Exception {
		File folderRoot = temporaryFolder.getRoot();
		File folderA = new File(folderRoot, String.valueOf(UUID.randomUUID()));
		Assert.assertTrue(folderA.mkdirs());
		SnapshotDirectory snapshotDirectory = new SnapshotDirectory(new Path(folderA.toURI()));
		Assert.assertTrue(snapshotDirectory.isSnapshotOngoing());
		Assert.assertNotNull(snapshotDirectory.completeSnapshotAndGetHandle());
		Assert.assertFalse(snapshotDirectory.isSnapshotOngoing());
		snapshotDirectory = new SnapshotDirectory(new Path(folderA.toURI()));
		Assert.assertTrue(snapshotDirectory.isSnapshotOngoing());
		snapshotDirectory.deleteIfIncompleteSnapshot();
		Assert.assertFalse(snapshotDirectory.isSnapshotOngoing());
	}

	/**
	 * Tests that temporary directories have the right behavior on completion and deletion.
	 */
	@Test
	public void temporary() throws Exception {
		File folderRoot = temporaryFolder.getRoot();
		File folder = new File(folderRoot, String.valueOf(UUID.randomUUID()));
		Assert.assertTrue(folder.mkdirs());
		Path folderPath = new Path(folder.toURI());
		SnapshotDirectory tmpSnapshotDirectory = SnapshotDirectory.temporary(folderPath);
		// temporary snapshot directories should not return a handle, because they will be deleted.
		Assert.assertNull(tmpSnapshotDirectory.completeSnapshotAndGetHandle());
		// check that the directory is deleted even after we called #completeSnapshotAndGetHandle.
		Assert.assertTrue(tmpSnapshotDirectory.deleteIfIncompleteSnapshot());
		Assert.assertFalse(folder.exists());
	}

}
