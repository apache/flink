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


public class HadoopDriverTestMultipleInputsITCase extends HadoopTestBase {
	protected String textPath;
	protected String textPath2;
	protected String resultPath;


	@Override
	protected void preSubmit() throws Exception {
		String[] splits = WordCountData.TEXT.split("\n");
		String[] split1 = new String[splits.length / 2];
		String[] split2 = new String[splits.length - split1.length];

		System.arraycopy(splits, 0 , split1, 0, split1.length);
		System.arraycopy(splits, split1.length , split2, 0, split2.length);

		textPath = createTempFile("text.txt", StringUtils.join(split1, "\n"));
		textPath2 = createTempFile("text2.txt", StringUtils.join(split2, "\n"));

		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(WordCountData.COUNTS, resultPath + "/1");
	}

	@Override
	protected void testProgram() throws Exception {
		HadoopWordCountVariations.MultipleInputsWordCount.main(new String[]{textPath, textPath2, resultPath});
	}
}
