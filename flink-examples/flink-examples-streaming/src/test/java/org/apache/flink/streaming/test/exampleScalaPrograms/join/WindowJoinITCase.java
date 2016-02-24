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

package org.apache.flink.streaming.test.exampleScalaPrograms.join;

import org.apache.flink.streaming.scala.examples.join.WindowJoin;
import org.apache.flink.streaming.examples.join.util.WindowJoinData;
import org.apache.flink.streaming.util.StreamingProgramTestBase;

public class WindowJoinITCase extends StreamingProgramTestBase {

	protected String gradesPath;
	protected String salariesPath;
	protected String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		gradesPath = createTempFile("gradesText.txt", WindowJoinData.GRADES_INPUT);
		salariesPath = createTempFile("salariesText.txt", WindowJoinData.SALARIES_INPUT);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		// since the two sides of the join might have different speed
		// the exact output can not be checked just whether it is well-formed
		// checks that the result lines look like e.g. Person(bob, 2, 2015)
		checkLinesAgainstRegexp(resultPath, "^Person\\([a-z]+,(\\d),(\\d)+\\)");
	}

	@Override
	protected void testProgram() throws Exception {
		WindowJoin.main(new String[]{
				"--grades", gradesPath,
				"--salaries", salariesPath,
				"--output", resultPath});
	}
}
