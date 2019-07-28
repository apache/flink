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


package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.state.filesystem.FsSegmentStateHandle;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SharedStateRegistryTest {

	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();

	/**
	 * Validate that all states can be correctly registered at the registry.
	 */
	@Test
	public void testRegistryNormal() {

		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		// register one state
		TestSharedState firstState = new TestSharedState("first");
		SharedStateRegistry.Result result = sharedStateRegistry.registerReference(firstState.getRegistrationKey(), firstState);
		assertEquals(1, result.getReferenceCount());
		assertTrue(firstState == result.getReference());
		assertFalse(firstState.isDiscarded());

		// register another state
		TestSharedState secondState = new TestSharedState("second");
		result = sharedStateRegistry.registerReference(secondState.getRegistrationKey(), secondState);
		assertEquals(1, result.getReferenceCount());
		assertTrue(secondState == result.getReference());
		assertFalse(firstState.isDiscarded());
		assertFalse(secondState.isDiscarded());

		// attempt to register state under an existing key
		TestSharedState firstStatePrime = new TestSharedState(firstState.getRegistrationKey().getKeyString());
		result = sharedStateRegistry.registerReference(firstState.getRegistrationKey(), firstStatePrime);
		assertEquals(2, result.getReferenceCount());
		assertFalse(firstStatePrime == result.getReference());
		assertTrue(firstState == result.getReference());
		assertTrue(firstStatePrime.isDiscarded());
		assertFalse(firstState.isDiscarded());

		// reference the first state again
		result = sharedStateRegistry.registerReference(firstState.getRegistrationKey(), firstState);
		assertEquals(3, result.getReferenceCount());
		assertTrue(firstState == result.getReference());
		assertFalse(firstState.isDiscarded());

		// unregister the second state
		result = sharedStateRegistry.unregisterReference(secondState.getRegistrationKey());
		assertEquals(0, result.getReferenceCount());
		assertTrue(result.getReference() == null);
		assertTrue(secondState.isDiscarded());

		// unregister the first state
		result = sharedStateRegistry.unregisterReference(firstState.getRegistrationKey());
		assertEquals(2, result.getReferenceCount());
		assertTrue(firstState == result.getReference());
		assertFalse(firstState.isDiscarded());
	}

	/**
	 * Validate that unregister a nonexistent key will throw exception
	 */
	@Test(expected = IllegalStateException.class)
	public void testUnregisterWithUnexistedKey() {
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		sharedStateRegistry.unregisterReference(new SharedStateRegistryKey("non-existent"));
	}

	@Test
	public void testRegisterForFsSegmentStateHandle() throws IOException {
		// We'll test the scenario that of 5 checkpoint for one operator.
		// checkpoint 1 includes 1.sst, 2.sst 3.sst
		// checkpoint 2 includes 2.sst 3.sst 4.sst
		// checkpoint 3 includes 3.sst 5.sst, 6.sst 7.sst
		// checkpoint 4 includes 6.sst, 7.sst, 8.sst 9.sst
		// checkpoint 5 includes 10.sst, 11.sst, 12.sst 13.sst

		String content = "Hello,World, It's 42!";

		FileSystem fs = new LocalFileSystem();
		// checkpoint 1, includes 1.sst, 2.sst 3.sst
		Path firstPath = createPathWithContent(folder, "first", content);
		FsSegmentStateHandle handle1 = new FsSegmentStateHandle(firstPath, 0, 3);
		FsSegmentStateHandle handle2 = new FsSegmentStateHandle(firstPath, 3, 8);
		FsSegmentStateHandle handle3 = new FsSegmentStateHandle(firstPath, 8, content.length());

		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		SharedStateRegistryKey firstKey = new SharedStateRegistryKey("1.sst");
		SharedStateRegistry.Result result = sharedStateRegistry.registerReference(firstKey, handle1);
		assertEquals(handle1, result.getReference());
		assertEquals(1, result.getReferenceCount());

		SharedStateRegistryKey secondKey = new SharedStateRegistryKey("2.sst");
		result = sharedStateRegistry.registerReference(secondKey, handle2);
		assertEquals(handle2, result.getReference());
		assertEquals(1, result.getReferenceCount());

		SharedStateRegistryKey thirdKey = new SharedStateRegistryKey("3.sst");
		result = sharedStateRegistry.registerReference(thirdKey, handle3);
		assertEquals(handle3, result.getReference());
		assertEquals(1, result.getReferenceCount());
		sharedStateRegistry.delUselessUnderlyingFile();

		assertTrue(fs.exists(firstPath));

		// checkpoint 2 includes 2.sst 3.sst 4.sst
		Path secondPath = createPathWithContent(folder, "second", content);
		result = sharedStateRegistry.registerReference(secondKey, handle2);
		assertEquals(handle2, result.getReference());
		assertEquals(2, result.getReferenceCount());

		result = sharedStateRegistry.registerReference(thirdKey, handle3);
		assertEquals(handle3, result.getReference());
		assertEquals(2, result.getReferenceCount());

		FsSegmentStateHandle handle4 = new FsSegmentStateHandle(secondPath, 0, content.length());
		SharedStateRegistryKey fourthKey = new SharedStateRegistryKey("4.sst");
		result = sharedStateRegistry.registerReference(fourthKey, handle4);
		assertEquals(handle4, result.getReference());
		assertEquals(1, result.getReferenceCount());
		sharedStateRegistry.delUselessUnderlyingFile();

		assertTrue(fs.exists(firstPath));
		assertTrue(fs.exists(secondPath));

		// try to unregister the state handle of checkpoint 1.
		result = sharedStateRegistry.unregisterReference(firstKey);
		assertNull(result.getReference());
		assertEquals(0, result.getReferenceCount());
		result = sharedStateRegistry.unregisterReference(secondKey);
		assertEquals(handle2, result.getReference());
		assertEquals(1, result.getReferenceCount());
		result = sharedStateRegistry.unregisterReference(thirdKey);
		assertEquals(handle3, result.getReference());
		assertEquals(1, result.getReferenceCount());

		// we have (2.sst, 3.sst) from checkpoint 1 and 4.sst from checkpoint 2
		assertTrue(fs.exists(firstPath));
		assertTrue(fs.exists(secondPath));

		// checkpoint 3 includes 3.sst 5.sst, 6.sst 7.sst
		Path thirdPath = createPathWithContent(folder, "third", content);
		// 3.sst
		result = sharedStateRegistry.registerReference(thirdKey, handle3);
		assertEquals(handle3, result.getReference());
		assertEquals(2, result.getReferenceCount());

		FsSegmentStateHandle handle5 = new FsSegmentStateHandle(thirdPath, 0, 4);
		SharedStateRegistryKey fivethKey = new SharedStateRegistryKey("5.sst");
		result = sharedStateRegistry.registerReference(fivethKey, handle5);
		assertEquals(handle5, result.getReference());
		assertEquals(1, result.getReferenceCount());

		FsSegmentStateHandle handle6 = new FsSegmentStateHandle(thirdPath, 4, 9);
		SharedStateRegistryKey sixthKey = new SharedStateRegistryKey("6.sst");
		result = sharedStateRegistry.registerReference(sixthKey, handle6);
		assertEquals(handle6, result.getReference());
		assertEquals(1, result.getReferenceCount());

		FsSegmentStateHandle handle7 = new FsSegmentStateHandle(thirdPath, 9, content.length());
		SharedStateRegistryKey seventhKey = new SharedStateRegistryKey("7.sst");
		result = sharedStateRegistry.registerReference(seventhKey, handle7);
		assertEquals(handle7, result.getReference());
		assertEquals(1, result.getReferenceCount());

		sharedStateRegistry.delUselessUnderlyingFile();

		assertTrue(fs.exists(firstPath));
		assertTrue(fs.exists(secondPath));
		assertTrue(fs.exists(thirdPath));

		// try to unregister state handle of checkpoint 2.
		result = sharedStateRegistry.unregisterReference(secondKey);
		assertNull(result.getReference());
		assertEquals(0, result.getReferenceCount());

		result = sharedStateRegistry.unregisterReference(thirdKey);
		assertEquals(handle3, result.getReference());
		assertEquals(1, result.getReferenceCount());

		result = sharedStateRegistry.unregisterReference(fourthKey);
		assertNull(result.getReference());
		assertEquals(0, result.getReferenceCount());

		// we have (3.sst) from checkpoint 1 and (5.sst, 6.sst, 7.sst) from checkpoint 3
		assertTrue(fs.exists(firstPath));
		assertFalse(fs.exists(secondPath));
		assertTrue(fs.exists(thirdPath));

		// checkpoint 4 includes 6.sst, 7.sst, 8.sst 9.sst
		Path fourthPath = createPathWithContent(folder, "fourth", content);
		// 6.sst
		result = sharedStateRegistry.registerReference(sixthKey, handle6);
		assertEquals(handle6, result.getReference());
		assertEquals(2, result.getReferenceCount());

		// 7.sst
		result = sharedStateRegistry.registerReference(seventhKey, handle7);
		assertEquals(handle7, result.getReference());
		assertEquals(2, result.getReferenceCount());

		FsSegmentStateHandle handle8 = new FsSegmentStateHandle(fourthPath, 0, 9);
		SharedStateRegistryKey eighthKey = new SharedStateRegistryKey("8.sst");
		result = sharedStateRegistry.registerReference(eighthKey, handle8);
		assertEquals(handle8, result.getReference());
		assertEquals(1, result.getReferenceCount());

		FsSegmentStateHandle handle9 = new FsSegmentStateHandle(fourthPath, 9, content.length());
		SharedStateRegistryKey ninethKey = new SharedStateRegistryKey("9.sst");
		result = sharedStateRegistry.registerReference(ninethKey, handle9);
		assertEquals(handle9, result.getReference());
		assertEquals(1, result.getReferenceCount());

		// try to unregister state handles of checkpoint 3.
		result = sharedStateRegistry.unregisterReference(thirdKey);
		assertNull(result.getReference());
		assertEquals(0, result.getReferenceCount());
		result = sharedStateRegistry.unregisterReference(fivethKey);
		assertNull(result.getReference());
		assertEquals(0, result.getReferenceCount());
		result = sharedStateRegistry.unregisterReference(sixthKey);
		assertEquals(handle6, result.getReference());
		assertEquals(1, result.getReferenceCount());
		result = sharedStateRegistry.unregisterReference(seventhKey);
		assertEquals(handle7, result.getReference());
		assertEquals(1, result.getReferenceCount());

		// we have (6.sst, 7.sst) from checkpoint 3, and (8.sst, 9.sst) from checkpoint 4
		assertFalse(fs.exists(firstPath));
		assertFalse(fs.exists(secondPath));
		assertTrue(fs.exists(thirdPath));
		assertTrue(fs.exists(fourthPath));

		// checkpoint 5 includes 10.sst, 11.sst, 12.sst 13.sst
		Path fivethPath = createPathWithContent(folder, "fiveth", content);
		FsSegmentStateHandle handle10 = new FsSegmentStateHandle(fivethPath, 0, 5);
		SharedStateRegistryKey tenthKey = new SharedStateRegistryKey("10.sst");
		result = sharedStateRegistry.registerReference(tenthKey, handle10);
		assertEquals(handle10, result.getReference());
		assertEquals(1, result.getReferenceCount());

		FsSegmentStateHandle handle11 = new FsSegmentStateHandle(fivethPath, 5, 8);
		SharedStateRegistryKey eleventhKey = new SharedStateRegistryKey("11.sst");
		result = sharedStateRegistry.registerReference(eleventhKey, handle11);
		assertEquals(handle11, result.getReference());
		assertEquals(1, result.getReferenceCount());

		FsSegmentStateHandle handle12 = new FsSegmentStateHandle(fivethPath, 8, 13);
		SharedStateRegistryKey twelvethKey = new SharedStateRegistryKey("12.sst");
		result = sharedStateRegistry.registerReference(twelvethKey, handle12);
		assertEquals(handle12, result.getReference());
		assertEquals(1, result.getReferenceCount());

		FsSegmentStateHandle handle13 = new FsSegmentStateHandle(fivethPath, 13, content.length());
		SharedStateRegistryKey thirteenthKey = new SharedStateRegistryKey("13.sst");
		result = sharedStateRegistry.registerReference(thirteenthKey, handle13);
		assertEquals(handle13, result.getReference());
		assertEquals(1, result.getReferenceCount());

		// try to unregister state handles of checkpoint 4.
		result = sharedStateRegistry.unregisterReference(sixthKey);
		assertNull(result.getReference());
		assertEquals(0, result.getReferenceCount());

		result = sharedStateRegistry.unregisterReference(seventhKey);
		assertNull(result.getReference());
		assertEquals(0, result.getReferenceCount());

		result = sharedStateRegistry.unregisterReference(eighthKey);
		assertNull(result.getReference());
		assertEquals(0, result.getReferenceCount());

		result = sharedStateRegistry.unregisterReference(ninethKey);
		assertNull(result.getReference());
		assertEquals(0, result.getReferenceCount());

		// we only have (10.sst, 11.sst, 12.sst, 13.sst) from checkpoint 5
		assertFalse(fs.exists(firstPath));
		assertFalse(fs.exists(secondPath));
		assertFalse(fs.exists(thirdPath));
		assertFalse(fs.exists(fourthPath));
		assertTrue(fs.exists(fivethPath));
	}

	@Test
	public void testRegisterForFsSegmentStateHandleConcurrentCheckpoint() throws IOException {
		// max concurrent checkpoint = 2, max retained checkpoint = 2
		// checkpoint 1 includes 1.sst, 2.sst, 3.sst
		// checkpoint 2 includes 2.sst, 3.sst, 4.sst
		// checkpoint 3 includes 4.sst
		// checkpoint 4 includes 4.sst, 5.sst
		// checkpoint 5 includes 4.sst, 6.sst, 5.sst
		// checkpoint 2 and checkpoint 3 are both do incremental based on checkpoint 1.
		// file generated in checkpoint 3 will be deleted(4.sst will be replace by the statehandle of checkpoint 2).
		// checkpoint 4 and checkpoint 5 are both do incremental based on checkpoint 3.
		// file generated in checkpoint 5 will exist(because 6.sst used it).
		String content = "Hello,World, It's 42!";

		FileSystem fs = new LocalFileSystem();
		// checkpoint 1(1.sst, 2.sst 3.sst)
		Path firstPath = createPathWithContent(folder, "first", content);
		FsSegmentStateHandle handle1 = new FsSegmentStateHandle(firstPath, 0, 3);
		FsSegmentStateHandle handle2 = new FsSegmentStateHandle(firstPath, 3, 8);
		FsSegmentStateHandle handle3 = new FsSegmentStateHandle(firstPath, 8, content.length());

		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		Map<String, Integer> fileRefCount = sharedStateRegistry.getFileRefCounts();
		SharedStateRegistryKey firstKey = new SharedStateRegistryKey("1.sst");
		SharedStateRegistry.Result result = sharedStateRegistry.registerReference(firstKey, handle1);
		assertEquals(handle1, result.getReference());
		assertEquals(1, result.getReferenceCount());

		SharedStateRegistryKey secondKey = new SharedStateRegistryKey("2.sst");
		result = sharedStateRegistry.registerReference(secondKey, handle2);
		assertEquals(handle2, result.getReference());
		assertEquals(1, result.getReferenceCount());

		SharedStateRegistryKey thirdKey = new SharedStateRegistryKey("3.sst");
		result = sharedStateRegistry.registerReference(thirdKey, handle3);
		assertEquals(handle3, result.getReference());
		assertEquals(1, result.getReferenceCount());
		sharedStateRegistry.delUselessUnderlyingFile();

		assertTrue(fs.exists(firstPath));
		assertEquals(1, fileRefCount.size());
		assertEquals((Integer) 3, fileRefCount.get(firstPath.toUri().toString()));

		// checkpoint 2(2.sst, 3.sst, 4.sst) based on checkpoint 1(1.sst, 2.sst, 3.sst)
		result = sharedStateRegistry.registerReference(secondKey, handle2);
		assertEquals(handle2, result.getReference());
		assertEquals(2, result.getReferenceCount());

		result = sharedStateRegistry.registerReference(thirdKey, handle3);
		assertEquals(handle3, result.getReference());
		assertEquals(2, result.getReferenceCount());

		Path secondPath = createPathWithContent(folder, "second", content);
		FsSegmentStateHandle handle4 = new FsSegmentStateHandle(secondPath, 0, content.length());
		SharedStateRegistryKey fourthKey = new SharedStateRegistryKey("4.sst");
		result = sharedStateRegistry.registerReference(fourthKey, handle4);
		assertEquals(handle4, result.getReference());
		assertEquals(1, result.getReferenceCount());
		sharedStateRegistry.delUselessUnderlyingFile();

		assertTrue(fs.exists(firstPath));
		assertTrue(fs.exists(secondPath));
		assertEquals(2, fileRefCount.size());
		assertEquals((Integer) 5, fileRefCount.get(firstPath.toUri().toString()));
		assertEquals((Integer) 1, fileRefCount.get(secondPath.toUri().toString()));

		// checkpoint 3(4.sst) based on checkpoint 1(1.sst, 2.sst, 3.sst)
		Path thirdPath = createPathWithContent(folder, "third", content);
		FsSegmentStateHandle handle43 = new FsSegmentStateHandle(thirdPath, 0, content.length());
		SharedStateRegistryKey fourthKey43 = new SharedStateRegistryKey("4.sst");
		result = sharedStateRegistry.registerReference(fourthKey43, handle43);
		assertEquals(handle4, result.getReference());
		assertEquals(2, result.getReferenceCount());
		sharedStateRegistry.delUselessUnderlyingFile();

		assertTrue(fs.exists(firstPath));
		assertTrue(fs.exists(secondPath));
		assertFalse(fs.exists(thirdPath));
		assertEquals(2, fileRefCount.size());
		assertEquals((Integer) 5, fileRefCount.get(firstPath.toUri().toString()));
		assertEquals((Integer) 2, fileRefCount.get(secondPath.toUri().toString()));

		// try do subsume checkpoint 1(1.sst, 2.sst, 3.sst).
		result = sharedStateRegistry.unregisterReference(firstKey);
		assertNull(result.getReference());
		assertEquals(0, result.getReferenceCount());
		result = sharedStateRegistry.unregisterReference(secondKey);
		assertEquals(handle2, result.getReference());
		assertEquals(1, result.getReferenceCount());
		result = sharedStateRegistry.unregisterReference(thirdKey);
		assertEquals(handle3, result.getReference());
		assertEquals(1, result.getReferenceCount());

		assertTrue(fs.exists(firstPath));
		assertTrue(fs.exists(secondPath));
		assertFalse(fs.exists(thirdPath));
		assertEquals(2, fileRefCount.size());
		assertEquals((Integer) 2, fileRefCount.get(firstPath.toUri().toString()));
		assertEquals((Integer) 2, fileRefCount.get(secondPath.toUri().toString()));

		// checkpoint 4(4.sst, 5.sst) based on checkpoint 3(4.sst)
		result = sharedStateRegistry.registerReference(fourthKey, handle4);
		assertEquals(handle4, result.getReference());
		assertEquals(3, result.getReferenceCount());
		Path fourthPath = createPathWithContent(folder, "four", content);
		FsSegmentStateHandle handle5 = new FsSegmentStateHandle(fourthPath, 0, content.length());
		SharedStateRegistryKey fivethKey = new SharedStateRegistryKey("5.sst");
		result = sharedStateRegistry.registerReference(fivethKey, handle5);
		assertEquals(handle5, result.getReference());
		assertEquals(1, result.getReferenceCount());

		sharedStateRegistry.delUselessUnderlyingFile();

		assertTrue(fs.exists(firstPath));
		assertTrue(fs.exists(secondPath));
		assertFalse(fs.exists(thirdPath));
		assertTrue(fs.exists(fourthPath));
		assertEquals(3, fileRefCount.size());
		assertEquals((Integer) 2, fileRefCount.get(firstPath.toUri().toString()));
		assertEquals((Integer) 3, fileRefCount.get(secondPath.toUri().toString()));
		assertEquals((Integer) 1, fileRefCount.get(fourthPath.toUri().toString()));

		// try to unregister checkpoint 2(2.sst, 3.sst, 4.sst)
		result = sharedStateRegistry.unregisterReference(secondKey);
		assertNull(result.getReference());
		assertEquals(0, result.getReferenceCount());
		result = sharedStateRegistry.unregisterReference(thirdKey);
		assertNull(result.getReference());
		assertEquals(0, result.getReferenceCount());
		result = sharedStateRegistry.unregisterReference(fourthKey);
		assertEquals(handle4, result.getReference());
		assertEquals(2, result.getReferenceCount());

		assertFalse(fs.exists(firstPath));
		assertTrue(fs.exists(secondPath));
		assertFalse(fs.exists(thirdPath));
		assertTrue(fs.exists(fourthPath));
		assertEquals(2, fileRefCount.size());
		assertEquals((Integer) 2, fileRefCount.get(secondPath.toUri().toString()));
		assertEquals((Integer) 1, fileRefCount.get(fourthPath.toUri().toString()));

		// checkpoint 5(4.sst, 5.sst, 6.sst) based on checkpoint 3(4.sst)
		Path fivethPath = createPathWithContent(folder, "five", content);
		FsSegmentStateHandle handle6 = new FsSegmentStateHandle(fivethPath, 8, content.length());
		SharedStateRegistryKey sixthKey = new SharedStateRegistryKey("6.sst");
		result = sharedStateRegistry.registerReference(sixthKey, handle6);
		assertEquals(handle6, result.getReference());
		assertEquals(1, result.getReferenceCount());
		result = sharedStateRegistry.registerReference(fourthKey, handle4);
		assertEquals(handle4, result.getReference());
		assertEquals(3, result.getReferenceCount());
		FsSegmentStateHandle handle55 = new FsSegmentStateHandle(fivethPath, 0, 8);
		result = sharedStateRegistry.registerReference(fivethKey, handle55);
		assertEquals(handle5, result.getReference());
		assertEquals(2, result.getReferenceCount());
		sharedStateRegistry.delUselessUnderlyingFile();

		assertFalse(fs.exists(firstPath));
		assertTrue(fs.exists(secondPath));
		assertFalse(fs.exists(thirdPath));
		assertTrue(fs.exists(fourthPath));
		assertTrue(fs.exists(fivethPath));
		assertEquals(3, fileRefCount.size());
		assertEquals((Integer) 3, fileRefCount.get(secondPath.toUri().toString()));
		assertEquals((Integer) 2, fileRefCount.get(fourthPath.toUri().toString()));
		assertEquals((Integer) 1, fileRefCount.get(fivethPath.toUri().toString()));

		// try to subsume checkpoint 3(4.sst)
		result = sharedStateRegistry.unregisterReference(fourthKey);
		assertEquals(handle4, result.getReference());
		assertEquals(2, result.getReferenceCount());

		assertFalse(fs.exists(firstPath));
		assertTrue(fs.exists(secondPath));
		assertFalse(fs.exists(thirdPath));
		assertTrue(fs.exists(fourthPath));
		assertTrue(fs.exists(fivethPath));
		assertEquals(3, fileRefCount.size());
		assertEquals((Integer) 2, fileRefCount.get(secondPath.toUri().toString()));
		assertEquals((Integer) 2, fileRefCount.get(fourthPath.toUri().toString()));
		assertEquals((Integer) 1, fileRefCount.get(fivethPath.toUri().toString()));
	}

	private Path createPathWithContent(TemporaryFolder folder, String fileName, String content) throws IOException {
		File file = folder.newFile(fileName);
		BufferedWriter writer = new BufferedWriter(new FileWriter(file.getAbsoluteFile(), true));
		writer.append(content);
		writer.close();

		return Path.fromLocalFile(file);
	}

	private static class TestSharedState implements StreamStateHandle {
		private static final long serialVersionUID = 4468635881465159780L;

		private SharedStateRegistryKey key;

		private boolean discarded;

		TestSharedState(String key) {
			this.key = new SharedStateRegistryKey(key);
			this.discarded = false;
		}

		public SharedStateRegistryKey getRegistrationKey() {
			return key;
		}

		@Override
		public void discardState() throws Exception {
			this.discarded = true;
		}

		@Override
		public long getStateSize() {
			return key.toString().length();
		}

		@Override
		public int hashCode() {
			return key.hashCode();
		}

		@Override
		public FSDataInputStream openInputStream() throws IOException {
			throw new UnsupportedOperationException();
		}

		public boolean isDiscarded() {
			return discarded;
		}
	}
}
