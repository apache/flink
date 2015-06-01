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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.flink.streaming.connectors.json.JSONParser;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.junit.Test;


public class JSONParserTest2 {
	
	@Test
	public void testGetBooleanFunction() {
		String jsonText = "{\"key\":true}";
		String searchedField = "key";
		try {
			JSONParser parser = new JSONParser(jsonText);
			JSONObject jo = parser.parse(searchedField);

			assertTrue(jo.getBoolean("retValue"));
		} 
		catch (JSONException e) {
			fail();
		}
	}
	
	@Test
	public void testGetDoubleFunction() {
		double expected = 12345.12345;
		String jsonText = "{\"key\":" + expected + "}";
		String searchedField = "key";
		try {
			JSONParser parser = new JSONParser(jsonText);
			JSONObject jo = parser.parse(searchedField);

			assertEquals(expected,jo.getDouble("retValue"),0.000001);
		} 
		catch (JSONException e) {
			fail();
		}
	}
	
	@Test
	public void testGetIntFunction() {
		int expected = 15;
		String jsonText = "{\"key\":" + expected + "}";
		String searchedField = "key";
		try {
			JSONParser parser = new JSONParser(jsonText);
			JSONObject jo = parser.parse(searchedField);

			assertEquals(expected,jo.getInt("retValue"));
		} 
		catch (JSONException e) {
			fail();
		}
	}

	@Test
	public void testGetLongFunction() {
		long expected = 111111111111L;
		String jsonText = "{\"key\":" + expected + "}";
		String searchedField = "key";
		try {
			JSONParser parser = new JSONParser(jsonText);
			JSONObject jo = parser.parse(searchedField);

			assertEquals(expected,jo.getLong("retValue"));
		} 
		catch (JSONException e) {
			fail();
		}
	}
	
}
