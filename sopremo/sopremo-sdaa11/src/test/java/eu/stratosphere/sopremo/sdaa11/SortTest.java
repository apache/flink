/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.sopremo.sdaa11;

import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

/**
 * @author skruse
 * 
 */
public class SortTest {

	@Test
	public void testSort() throws IOException {
		final SopremoTestPlan plan = new SopremoTestPlan(new Sort());

		final String input = "[" + "[]," + "[\"a\"]," + "[\"a\", \"b\"],"
				+ "[\"b\", \"a\"]," + "[\"a\", \"b\", \"c\"],"
				+ "[\"b\", \"a\", \"b\"]," + "[\"c\", \"b\", \"a\"]" + "]";
		JsonParser parser = new JsonParser(input);
		while (!parser.checkEnd())
			plan.getInput(0).add(parser.readValueAsTree());

		final String output = "[" + "[]," + "[\"a\"]," + "[\"a\", \"b\"],"
				+ "[\"a\", \"b\"]," + "[\"a\", \"b\", \"c\"],"
				+ "[\"a\", \"b\", \"b\"]," + "[\"a\", \"b\", \"c\"]" + "]";
		parser = new JsonParser(output);
		while (!parser.checkEnd())
			plan.getExpectedOutput(0).add(parser.readValueAsTree());

		plan.run();

	}
}
