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

package org.apache.flink.test.hadoopcompatibility.mapred;

import org.apache.flink.test.hadoopcompatibility.mapred.example.HadoopMapredCompatWordCount;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.OperatingSystem;

import org.junit.Assume;
import org.junit.Before;

/**
 * IT cases for mapred.
 */
public class HadoopMapredITCase extends JavaProgramTestBase {

	protected String textPath;
	protected String resultPath;

	@Before
	public void checkOperatingSystem() {
		// FLINK-5164 - see https://wiki.apache.org/hadoop/WindowsProblems
		Assume.assumeTrue("This test can't run successfully on Windows.", !OperatingSystem.isWindows());
	}

	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("text.txt", WordCountData.TEXT);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(WordCountData.COUNTS, resultPath, new String[]{".", "_"});
	}

	@Override
	protected void testProgram() throws Exception {
		HadoopMapredCompatWordCount.main(new String[] { textPath, resultPath });
	}

}
