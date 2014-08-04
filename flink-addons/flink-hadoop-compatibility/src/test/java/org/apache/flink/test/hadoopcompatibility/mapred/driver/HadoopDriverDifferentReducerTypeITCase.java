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

package org.apache.flink.test.hadoopcompatibility.mapred.driver;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.JavaProgramTestBase;

public class HadoopDriverDifferentReducerTypeITCase extends JavaProgramTestBase{

	protected String textPath;
	protected String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("text.txt", WordCountData.TEXT);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		final String[] counts = WordCountData.COUNTS.split("\n");
		final String[] hashcodeCounts = new String[counts.length];
		for (int i=0; i < counts.length; i++) {
			final String[] curKeyValue = counts[i].split(" ");
			hashcodeCounts[i] = curKeyValue[0].hashCode() + " " + curKeyValue[1];
		}
		compareResultsByLinesInMemory(StringUtils.join(hashcodeCounts, "\n"), resultPath + "/1");
	}

	@Override
	protected void testProgram() throws Exception {
		HadoopWordCountVariations.WordCountDifferentReducerTypes.main(new String[]{textPath, resultPath});
	}

}
