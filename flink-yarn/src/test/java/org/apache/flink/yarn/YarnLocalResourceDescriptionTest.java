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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link YarnLocalResourceDescriptor}.
 */
@RunWith(Parameterized.class)
public class YarnLocalResourceDescriptionTest extends TestLogger {

	private final long ts = System.currentTimeMillis();

	@Parameterized.Parameter
	public String pathStr;

	@Parameterized.Parameter(1)
	public long length;

	@Parameterized.Parameters(name = "pathStr = {0}, length = {1}")
	public static Object[] parameters() {
		return new Object[][]{
				new Object[]{"hdfs://nn/tmp/flink.jar", 100 * 1024 * 1024L},
				new Object[]{"https://flink/test/flink.jar", -1L}
		};
	}

	@Test
	public void testFromString() throws Exception {
		Path path = new Path(pathStr);
		final YarnLocalResourceDescriptor localResourceDesc = new YarnLocalResourceDescriptor(
			path.getName(),
			path,
			length,
			ts,
			LocalResourceVisibility.PUBLIC,
			LocalResourceType.FILE);

		final String desc = localResourceDesc.toString();
		YarnLocalResourceDescriptor newLocalResourceDesc = YarnLocalResourceDescriptor.fromString(desc);
		assertThat(newLocalResourceDesc.getResourceKey(), is(path.getName()));
		assertThat(newLocalResourceDesc.getPath(), is(path));
		assertThat(newLocalResourceDesc.getSize(), is(length));
		assertThat(newLocalResourceDesc.getModificationTime(), is(ts));
		assertThat(newLocalResourceDesc.getVisibility(), is(LocalResourceVisibility.PUBLIC));
		assertThat(newLocalResourceDesc.getResourceType(), is(LocalResourceType.FILE));
	}

	@Test
	public void testFromStringMalformed() {
		Path path = new Path(pathStr);
		final String desc = String.format(
			"YarnLocalResourceDescriptor{key=%s path=%s size=%d modTime=%d visibility=%s}",
			path.getName(),
			path.toString(),
			length,
			ts,
			LocalResourceVisibility.PUBLIC);
		assertThrows(
			"Error to parse YarnLocalResourceDescriptor from " + desc,
			FlinkException.class,
			() -> YarnLocalResourceDescriptor.fromString(desc)
		);
	}
}
