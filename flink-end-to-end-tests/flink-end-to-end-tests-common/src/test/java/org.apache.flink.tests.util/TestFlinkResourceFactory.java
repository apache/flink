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

package org.apache.flink.tests.util;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestFlinkResourceFactory {

	@After
	public void tearDown() {
		System.clearProperty(FlinkResourceFactory.E2E_FLINK_MODE);
		System.clearProperty(FlinkResourceFactory.E2E_FLINK_DIST_DIR);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreateResourceWithoutDistDir() {
		System.setProperty(FlinkResourceFactory.E2E_FLINK_MODE, FlinkResourceFactory.E2E_FLINK_MODE_LOCAL_STANDALONE);
		FlinkResource resource = FlinkResourceFactory.create();
		Assert.assertTrue(resource instanceof LocalStandaloneFlinkResource);
	}

	@Test
	public void testCreateFlinkLocalStandaloneResource() {
		System.setProperty(FlinkResourceFactory.E2E_FLINK_MODE, FlinkResourceFactory.E2E_FLINK_MODE_LOCAL_STANDALONE);
		System.setProperty(FlinkResourceFactory.E2E_FLINK_DIST_DIR, ".");
		FlinkResource resource = FlinkResourceFactory.create();
		Assert.assertTrue(resource instanceof LocalStandaloneFlinkResource);
	}

	@Test
	public void testCreateFlinkDistributedResource() {
		System.setProperty(FlinkResourceFactory.E2E_FLINK_MODE, FlinkResourceFactory.E2E_FLINK_MODE_DISTRIBUTED);
		System.setProperty(FlinkResourceFactory.E2E_FLINK_DIST_DIR, ".");
		FlinkResource resource = FlinkResourceFactory.create();
		Assert.assertTrue(resource instanceof DistributionFlinkResource);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidFlinkMode() {
		System.setProperty(FlinkResourceFactory.E2E_FLINK_MODE, "InvalidMode");
		FlinkResourceFactory.create();
	}
}
