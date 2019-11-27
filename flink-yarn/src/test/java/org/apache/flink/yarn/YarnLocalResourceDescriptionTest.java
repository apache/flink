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

package org.apache.flink.yarn;

import org.apache.flink.util.TestLogger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link YarnLocalResourceDescription}.
 */
public class YarnLocalResourceDescriptionTest extends TestLogger {

	@Test
	public void testFromString() throws Exception {
		final String key = "flink.jar";
		final Path path = new Path("hdfs://nn/tmp/flink.jar");
		final long size = 100 * 1024 * 1024;
		final long ts = System.currentTimeMillis();
		YarnLocalResourceDescription localResourceDesc = new YarnLocalResourceDescription(
			key,
			path,
			size,
			ts,
			LocalResourceVisibility.PUBLIC);

		String desc = localResourceDesc.toString();
		YarnLocalResourceDescription newLocalResourceDesc = YarnLocalResourceDescription.fromString(desc);
		assertEquals(key, newLocalResourceDesc.getResourceKey());
		assertEquals(path, newLocalResourceDesc.getPath());
		assertEquals(size, newLocalResourceDesc.getSize());
		assertEquals(ts, newLocalResourceDesc.getModificationTime());
		assertEquals(LocalResourceVisibility.PUBLIC, newLocalResourceDesc.getVisibility());
	}
}
