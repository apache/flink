/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.test.testdata;

import org.junit.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.regex.Pattern;

public class TransitiveClosureData {

	public static void checkOddEvenResult(BufferedReader result) throws IOException {
		Pattern split = Pattern.compile(" ");
		String line;
		while ((line = result.readLine()) != null) {
			String[] res = split.split(line);
			Assert.assertEquals("Malformed result: Wrong number of tokens in line.", 2, res.length);
			try {
				int from = Integer.parseInt(res[0]);
				int to = Integer.parseInt(res[1]);

				Assert.assertEquals("Vertex should not be reachable.", from % 2, to % 2);
			} catch (NumberFormatException e) {
				Assert.fail("Malformed result.");
			}
		}
	}

	private TransitiveClosureData() {}
}
