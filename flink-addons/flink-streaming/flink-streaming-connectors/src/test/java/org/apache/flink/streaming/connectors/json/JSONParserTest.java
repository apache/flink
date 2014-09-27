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

package org.apache.flink.streaming.connectors.json;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;

import org.apache.flink.streaming.connectors.json.JSONParser;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class JSONParserTest {

	private String jsonText;
	private String searchedField;

	public JSONParserTest(String text, String field) {
		jsonText = text;
		searchedField = field;
	}

	@Parameters
	public static Collection<Object[]> initParameterList() {

		Object[][] parameterList = new Object[][] { 
				{ "{\"key\":\"value\"}", 							"key" },
				{ "{\"key\":[\"value\"]}", 							"key[0]" },
				{ "{\"key\":[{\"key\":\"value\"}]}", 				"key[0].key" },
				{ "{\"key\":[{\"key\":[{\"key\":\"value\"}]}]}", 	"key[0].key[0].key"},
				{ "{\"key\":[1,[{\"key\":\"value\"}]]}", 			"key[1][0].key" },
				{ "{\"key\":[1,[[\"key\",2,\"value\"]]]}", 			"key[1][0][2]" },
				{ "{\"key\":{\"key\":{\"otherKey\":\"wrongValue\",\"key\":\"value\"},\"otherKey\":\"wrongValue\"},\"otherKey\":\"wrongValue\"}" , "key.key.key"}
				};

		return Arrays.asList(parameterList);
	}

	@Test
	public void test() {
		try {
			JSONParser parser = new JSONParser(jsonText);
			JSONObject jo = parser.parse(searchedField);
			String expected = "{\"retValue\":\"value\"}";

			assertTrue(expected.equals(jo.toString()));
		} 
		catch (JSONException e) {
			fail();
		}
	}
}
