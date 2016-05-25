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
package org.apache.flink.streaming.connectors.rethinkdb;

import static org.junit.Assert.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for string serializer schema class
 */
public class StringJSONSerializationSchemaTest {

	protected StringJSONSerializationSchema schema;
	
	@Before
	public void setUp() {
		schema = new StringJSONSerializationSchema();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testJSONObject() throws Exception {
		JSONObject json = new JSONObject();
		json.put("key1", "value1");
		
		JSONObject jsonNested = new JSONObject();
		jsonNested.put("nest1", "nestedValue1");
		
		json.put("nested", jsonNested);
		
		JSONObject result = (JSONObject) schema.toJSON(json.toString());
		assertEquals("Json objects should be same", json, result);
	}
	
	@Test(expected=ParseException.class)
	public void testJSONObjectBadInput() throws Exception {
		@SuppressWarnings("unused")
		JSONObject result = (JSONObject) schema.toJSON("bad json");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testJSONArray() throws Exception {
		JSONArray array = new JSONArray();
		JSONObject json1 = new JSONObject();
		json1.put("key1", "value1");
		
		JSONObject json2 = new JSONObject();
		json2.put("key2", "value2");
		
		array.add(json1);
		array.add(json2);
		
		JSONArray result = (JSONArray) schema.toJSON(array.toString());
		assertEquals("Json objects should be same", array, result);
	}
}
