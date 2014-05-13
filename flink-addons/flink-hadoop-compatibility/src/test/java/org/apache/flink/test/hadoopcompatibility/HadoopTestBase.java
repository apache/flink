/**
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

package org.apache.flink.test.hadoopcompatibility;

import org.apache.flink.test.util.JavaProgramTestBase;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;


public abstract class HadoopTestBase extends JavaProgramTestBase {

	/**
	 * Hadoop tests should not sort the result.
	 */
	public void compareResultsByLinesInMemory(String expectedResultStr, String resultPath) throws Exception {
		ArrayList<String> list = new ArrayList<String>();
		readAllResultLines(list, resultPath, false);

		String[] result = list.toArray(new String[list.size()]);
		String[] expected = expectedResultStr.isEmpty() ? new String[0] : expectedResultStr.split("\n");
		Arrays.sort(expected);

		Assert.assertEquals("Different number of lines in expected and obtained result.", expected.length, result.length);
		Assert.assertArrayEquals(expected, result);
	}

}
