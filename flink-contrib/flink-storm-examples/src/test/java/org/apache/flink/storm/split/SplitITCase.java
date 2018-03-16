/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.storm.split;

import org.apache.flink.storm.split.SpoutSplitExample.Enrich;
import org.apache.flink.storm.split.operators.VerifyAndEnrichBolt;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Tests for split examples.
 */
public class SplitITCase extends AbstractTestBase {

	private String output;

	@Before
	public void prepare() throws IOException {
		output = getTempFilePath("dummy").split(":")[1];
	}

	@After
	public void cleanUp() throws IOException {
		deleteRecursively(new File(output));
	}

	@Test
	public void testEmbeddedSpout() throws Exception {
		SpoutSplitExample.main(new String[] { "0", output });
		Assert.assertFalse(VerifyAndEnrichBolt.errorOccured);
		Assert.assertFalse(Enrich.errorOccured);
	}

	@Test
	public void testSpoutSplitTopology() throws Exception {
		SplitStreamSpoutLocal.main(new String[] { "0", output });
		Assert.assertFalse(VerifyAndEnrichBolt.errorOccured);
	}

	@Test
	public void testBoltSplitTopology() throws Exception {
		SplitStreamBoltLocal.main(new String[] { "0", output });
		Assert.assertFalse(VerifyAndEnrichBolt.errorOccured);
	}

}
