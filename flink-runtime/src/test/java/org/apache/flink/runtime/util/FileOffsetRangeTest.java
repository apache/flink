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

package org.apache.flink.runtime.util;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class FileOffsetRangeTest {

	final long fileSize = 1000L;
	final FileOffsetRange fo1 = (new FileOffsetRange(1000, 200)).normalize(fileSize);
	final FileOffsetRange fo2 = (new FileOffsetRange(700, 300)).normalize(fileSize);
	final FileOffsetRange fo3 = (new FileOffsetRange(700, 500)).normalize(fileSize);
	final FileOffsetRange fo4 = (new FileOffsetRange(0, 10000)).normalize(fileSize);
	final FileOffsetRange fo5 = (new FileOffsetRange(-500, 200)).normalize(fileSize);
	final FileOffsetRange fo6 = (new FileOffsetRange(-500, 10000)).normalize(fileSize);
	final FileOffsetRange fo7 = (new FileOffsetRange(-1000, 100)).normalize(fileSize);
	final FileOffsetRange fo8 = (new FileOffsetRange(-1100, 100)).normalize(fileSize);
	final FileOffsetRange fo9 = (new FileOffsetRange(1100, 100)).normalize(fileSize);

	@Test
	public void testAdapter(){
		checkFileOffsetRange(fo1, 800L, 200L);
		checkFileOffsetRange(fo2, 700L, 300L);
		checkFileOffsetRange(fo3, 700L, 300L);
		checkFileOffsetRange(fo4, 0L, 1000L);
		checkFileOffsetRange(fo5, 500L, 200L);
		checkFileOffsetRange(fo6, 500L, 500L);
		checkFileOffsetRange(fo7, 0L, 100L);
		checkFileOffsetRange(fo8, 0L, 100L);
		checkFileOffsetRange(fo9, 900L, 100L);
	}

	private void checkFileOffsetRange(FileOffsetRange fo, long start, long size){
		assertTrue(fo.getStart() == start && fo.getSize() == size);
	}

}
