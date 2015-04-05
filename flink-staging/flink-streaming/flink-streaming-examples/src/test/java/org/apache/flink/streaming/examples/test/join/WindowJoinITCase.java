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

package org.apache.flink.streaming.examples.test.join;

import org.apache.flink.streaming.examples.join.WindowJoin;
import org.apache.flink.streaming.examples.join.util.WindowJoinData;
import org.apache.flink.streaming.util.StreamingProgramTestBase;

public class WindowJoinITCase extends StreamingProgramTestBase {

	protected String gradesPath;
	protected String salariesPath;
	protected String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		setParallelism(1);
		gradesPath = createTempFile("gradesText.txt", WindowJoinData.GRADES_INPUT);
		salariesPath = createTempFile("salariesText.txt", WindowJoinData.SALARIES_INPUT);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(WindowJoinData.WINDOW_JOIN_RESULTS, resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		WindowJoin.main(new String[]{gradesPath, salariesPath, resultPath});
	}
}
