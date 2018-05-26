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

package org.apache.flink.storm.join;

import org.apache.flink.test.util.AbstractTestBase;

import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;

import org.junit.Test;

/**
 * Test for the SingleJoin example.
 */
public class SingleJoinITCase extends AbstractTestBase {

	protected static String[] expectedOutput = {
			"(male,20)",
			"(female,21)",
			"(male,22)",
			"(female,23)",
			"(male,24)",
			"(female,25)",
			"(male,26)",
			"(female,27)",
			"(male,28)",
			"(female,29)"
	};

	@Test
	public void testProgram() throws Exception {
		String resultPath = getTempDirPath("result");
		// We need to remove the file scheme because we can't use the Flink file system.
		// (to remain compatible with Storm)
		SingleJoinExample.main(new String[]{resultPath.replace("file:", "")});

		compareResultsByLinesInMemory(Joiner.on("\n").join(expectedOutput), resultPath);
	}

}
