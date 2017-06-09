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

package org.apache.flink.storm.wordcount;

import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.apache.flink.test.testdata.WordCountData;

/**
 * Test for the BoltTokenizerWordCountWithNames example.
 */
public class BoltTokenizerWordCountWithNamesITCase extends StreamingProgramTestBase {

	protected String textPath;
	protected String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		this.textPath = this.createTempFile("text.txt", WordCountData.TEXT);
		this.resultPath = this.getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(WordCountData.STREAMING_COUNTS_AS_TUPLES, this.resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		BoltTokenizerWordCountWithNames.main(new String[]{this.textPath, this.resultPath});
	}

}
