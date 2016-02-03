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

package org.apache.flink.streaming.test.exampleJavaPrograms.iteration;

import org.apache.flink.streaming.examples.iteration.IterateExample;
import org.apache.flink.streaming.examples.iteration.util.IterateExampleData;
import org.apache.flink.streaming.util.StreamingProgramTestBase;

public class IterateExampleITCase extends StreamingProgramTestBase {


	protected String inputPath;
	protected String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		inputPath = createTempFile("fibonacciInput.txt", IterateExampleData.INPUT_PAIRS);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		// the example is inherently non-deterministic. The iteration timeout of 5000 ms
		// is frequently not enough to make the test run stable on CI infrastructure
		// with very small containers, so we cannot do a validation here
	}

	@Override
	protected void testProgram() throws Exception {
		IterateExample.main(new String[]{inputPath, resultPath});
	}
}
