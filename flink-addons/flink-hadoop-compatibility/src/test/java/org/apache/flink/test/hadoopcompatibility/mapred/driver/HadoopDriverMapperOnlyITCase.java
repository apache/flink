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
import org.apache.flink.test.hadoopcompatibility.HadoopTestBase;
import org.apache.flink.test.testdata.WordCountData;

public class HadoopDriverMapperOnlyITCase extends HadoopTestBase {

	protected String textPath;
	protected String resultPath;

	private static final String RESULT = "alter 1\n" +
			"brudersphaeren 1\n" +
			"der 1\n" +
			"der 1\n" +
			"die 1\n" +
			"die 1\n" +
			"die 1\n" +
			"drei 1\n" +
			"erster 1\n" +
			"erzengel 1\n" +
			"faust 1\n" +
			"goethe 1\n" +
			"heerscharen 1\n" +
			"herr 1\n" +
			"himmel 1\n" +
			"himmlischen 1\n" +
			"im 1\n" +
			"in 1\n" +
			"mephistopheles 1\n" +
			"nach 1\n" +
			"nachher 1\n" +
			"prolog 1\n" +
			"raphael 1\n" +
			"sonne 1\n" +
			"teil 1\n" +
			"toent 1\n" +
			"tragoedie 1\n" +
			"treten 1\n" +
			"vor 1\n" +
			"weise 1\n" +
			"wettgesang 1";


	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("text.txt", WordCountData.TEXT.substring(0,
				StringUtils.ordinalIndexOf(WordCountData.TEXT, "\n", 5)));
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(RESULT, resultPath + "/1");
	}

	@Override
	protected void testProgram() throws Exception {
		HadoopWordCountVariations.StringTokenizer.main(new String[]{textPath, resultPath});
	}
}

