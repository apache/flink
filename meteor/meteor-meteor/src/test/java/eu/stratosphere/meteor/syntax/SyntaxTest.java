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

import junit.framework.Assert;
import eu.stratosphere.meteor.MeteorTest;
import eu.stratosphere.meteor.QueryParser;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.query.AbstractQueryParser;
import eu.stratosphere.sopremo.query.QueryParserException;

/**
 * @author Arvid Heise
 */
public class SyntaxTest extends MeteorTest {

	/**
	 * @param query
	 *        the query to test
	 * @param error
	 *        the expected error message
	 */
	protected void assertParserError(String query, String partialMessage) {
		try {
			SopremoPlan plan = new QueryParser().tryParse(query);
			Assert.fail("Parsing did not fail but produced " + plan);
		} catch (QueryParserException e) {
			if (e.getRawMessage().equals(AbstractQueryParser.DEFAULT_ERROR_MESSAGE)) {
				e.printStackTrace();
				Assert.fail("Generic fail message " + e);
			} else
				Assert.assertTrue(String.format("Expected parse error message to contain %s, but was %s", partialMessage, e.getRawMessage()), e.getRawMessage().contains(partialMessage));
		}
	}
}
