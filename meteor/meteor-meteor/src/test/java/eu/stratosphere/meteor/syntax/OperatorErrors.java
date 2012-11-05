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
package eu.stratosphere.meteor.syntax;

import org.junit.Test;

/**
 * @author Arvid Heise
 */
public class OperatorErrors extends SyntaxTest {
	@Test
	public void testMisspelledFilter() {
		String query = "$input = read from 'input.json';\n" +
			"$result = fitler $emp in $input where $emp.mgr or $emp.income > 30000;\n" +
			"write $result to 'output.json';";

		assertParserError(query, "filter");
	}

	@Test
	public void testUnknownOperator() {
		String query = "$input = read from 'input.json';\n" +
			"$result = xqwzts $emp in $input where $emp.mgr or $emp.income > 30000;\n" +
			"write $result to 'output.json';";

		assertParserError(query, "xqwzts");
	}

	@Test
	public void testMisspelledProperty() {
		String query = "$input = read from 'input.json';\n" +
			"$result = filter $emp in $input wher $emp.mgr or $emp.income > 30000;\n" +
			"write $result to 'output.json';";

		assertParserError(query, "where");
	}

	@Test
	public void testUnknownProperty() {
		String query = "$input = read from 'input.json';\n" +
			"$result = filter $emp in $input xqwzts $emp.mgr or $emp.income > 30000;\n" +
			"write $result to 'output.json';";

		assertParserError(query, "xqwzts");
	}
}
