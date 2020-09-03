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

import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.junit.Test;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link YarnLocalResourceDescriptor}.
 */
public class YarnLocalResourceDescriptionTest extends TestLogger {

	private final String key = "flink.jar";
	private final Path path = new Path("hdfs://nn/tmp/flink.jar");
	private final long size = 100 * 1024 * 1024;
	private final long ts = System.currentTimeMillis();

	@Test
	public void testFromString() throws Exception {
		final YarnLocalResourceDescriptor localResourceDesc = new YarnLocalResourceDescriptor(
			key,
			path,
			size,
			ts,
			LocalResourceVisibility.PUBLIC,
			LocalResourceType.FILE);

		final String desc = localResourceDesc.toString();
		YarnLocalResourceDescriptor newLocalResourceDesc = YarnLocalResourceDescriptor.fromString(desc);
		assertThat(newLocalResourceDesc.getResourceKey(), is(key));
		assertThat(newLocalResourceDesc.getPath(), is(path));
		assertThat(newLocalResourceDesc.getSize(), is(size));
		assertThat(newLocalResourceDesc.getModificationTime(), is(ts));
		assertThat(newLocalResourceDesc.getVisibility(), is(LocalResourceVisibility.PUBLIC));
		assertThat(newLocalResourceDesc.getResourceType(), is(LocalResourceType.FILE));
	}

	@Test
	public void testFromStringMalformed() {
		final String desc = String.format(
			"YarnLocalResourceDescriptor{key=%s path=%s size=%d modTime=%d visibility=%s}",
			key,
			path.toString(),
			size,
			ts,
			LocalResourceVisibility.PUBLIC);
		assertThrows(
			"Error to parse YarnLocalResourceDescriptor from " + desc,
			FlinkException.class,
			() -> YarnLocalResourceDescriptor.fromString(desc)
		);
	}
}
