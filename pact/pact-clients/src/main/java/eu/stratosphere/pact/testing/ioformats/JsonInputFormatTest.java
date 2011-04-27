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

package eu.stratosphere.pact.testing.ioformats;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.codehaus.jackson.JsonNode;
import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactLong;

/**
 * Tests {@link JsonInputFormat}.
 * 
 * @author Arvid Heise
 */
public class JsonInputFormatTest {
	/**
	 * @throws IOException
	 */
	@Test
	public void shouldProperlyReadArray() throws IOException {
		File file = File.createTempFile("jsonInputFormatTest", null);
		file.delete();
		OutputStreamWriter jsonWriter = new OutputStreamWriter(new FileOutputStream(file));
		jsonWriter.write("[{\"id\": 1}, {\"id\": 2}, {\"id\": 3}, {\"id\": 4}, {\"id\": 5}]");
		jsonWriter.close();

		JsonInputFormat inputFormat = FormatUtil.createInputFormat(JsonInputFormat.class, file.toURI()
			.toString(), null);
		KeyValuePair<PactLong, PactJsonObject> pair = inputFormat.createPair();
		for (int index = 1; index <= 5; index++) {
			Assert.assertFalse("more pairs expected @ " + index, inputFormat.reachedEnd());
			Assert.assertTrue("valid pair expected @ " + index, inputFormat.nextPair(pair));
			Assert
				.assertEquals("other order expected", index, pair.getValue().getValue().get("id").getIntValue());
		}

		if (!inputFormat.reachedEnd()) {
			Assert.assertTrue("no more pairs but reachedEnd did not return false", inputFormat.nextPair(pair));
			Assert.fail("pair unexpected: " + pair);
		}
	}

	/**
	 * @throws IOException
	 */
	@Test
	public void shouldProperlyReadSingleValue() throws IOException {
		File file = File.createTempFile("jsonInputFormatTest", null);
		file.delete();
		OutputStreamWriter jsonWriter = new OutputStreamWriter(new FileOutputStream(file));
		jsonWriter.write("{\"array\": [{\"id\": 1}, {\"id\": 2}, {\"id\": 3}, {\"id\": 4}, {\"id\": 5}]}");
		jsonWriter.close();

		JsonInputFormat inputFormat = FormatUtil.createInputFormat(JsonInputFormat.class, file.toURI()
			.toString(), null);
		KeyValuePair<PactLong, PactJsonObject> pair = inputFormat.createPair();

		if (!inputFormat.reachedEnd()) {
			if (!inputFormat.nextPair(pair))
				Assert.fail("one value expected expected: " + pair);
		}

		if (!inputFormat.reachedEnd()) {
			Assert.assertTrue("no more values but reachedEnd did not return false", inputFormat.nextPair(pair));
			Assert.fail("value unexpected: " + pair);
		}

		JsonNode arrayNode = pair.getValue().getValue().get("array");
		Assert.assertNotNull("could not find top level node", arrayNode);
		for (int index = 1; index <= 5; index++) {
			Assert.assertNotNull("could not find array element " + index, arrayNode.get(index - 1));
			Assert.assertEquals("other order expected", index, arrayNode.get(index - 1).get("id").getIntValue());
		}
	}
}
