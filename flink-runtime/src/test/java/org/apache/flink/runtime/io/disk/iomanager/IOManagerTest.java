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

package org.apache.flink.runtime.io.disk.iomanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel.ID;
import org.junit.Test;

public class IOManagerTest {

	@Test
	public void channelEnumerator() {
		File tempPath = new File(System.getProperty("java.io.tmpdir"));
		
		String[] tempDirs = new String[] {
			new File(tempPath, "a").getAbsolutePath(),
			new File(tempPath, "b").getAbsolutePath(),
			new File(tempPath, "c").getAbsolutePath(),
			new File(tempPath, "d").getAbsolutePath(),
			new File(tempPath, "e").getAbsolutePath(),
		};
		
		int[] counters = new int[tempDirs.length];
		
		
		FileIOChannel.Enumerator enumerator = new TestIOManager(tempDirs).createChannelEnumerator();

		for (int i = 0; i < 3 * tempDirs.length; i++) {
			FileIOChannel.ID id = enumerator.next();
			
			File path = new File(id.getPath());
			
			assertTrue("Channel IDs must name an absolute path.", path.isAbsolute());
			
			assertFalse("Channel IDs must name a file, not a directory.", path.isDirectory());
			
			assertTrue("Path is not in the temp directory.", tempPath.equals(path.getParentFile().getParentFile()));
			
			for (int k = 0; k < tempDirs.length; k++) {
				if (path.getParent().equals(tempDirs[k])) {
					counters[k]++;
				}
			}
		}
		
		for (int k = 0; k < tempDirs.length; k++) {
			assertEquals(3, counters[k]);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static class TestIOManager extends IOManager {

		protected TestIOManager(String[] paths) {
			super(paths);
		}

		@Override
		public void shutdown() {}

		@Override
		public boolean isProperlyShutDown() {
			return false;
		}

		@Override
		public BlockChannelWriter createBlockChannelWriter(ID channelID, LinkedBlockingQueue<MemorySegment> returnQueue) {
			throw new UnsupportedOperationException();
		}

		@Override
		public BlockChannelWriterWithCallback createBlockChannelWriter(ID channelID, RequestDoneCallback callback) {
			throw new UnsupportedOperationException();
		}

		@Override
		public BlockChannelReader createBlockChannelReader(ID channelID, LinkedBlockingQueue<MemorySegment> returnQueue) {
			throw new UnsupportedOperationException();
		}

		@Override
		public BulkBlockChannelReader createBulkBlockChannelReader(ID channelID, List<MemorySegment> targetSegments, int numBlocks) {
			throw new UnsupportedOperationException();
		}
	}
}