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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.sling.commons.json.JSONException;

/**
 * Abstract class derived from {@link RichFlatMapFunction} to handle JSON files.
 * 
 * @param <IN>
 *            Type of the input elements.
 * @param <OUT>
 *            Type of the returned elements.
 */
public abstract class JSONParseFlatMap<IN, OUT> extends RichFlatMapFunction<IN, OUT> {

	private static final long serialVersionUID = 1L;

	// private static final Log LOG = LogFactory.getLog(JSONParseFlatMap.class);

	/**
	 * Get the value object associated with a key form a JSON code. It can find
	 * embedded fields, too.
	 * 
	 * @param jsonText
	 *            JSON String in which the field is searched.
	 * @param field
	 *            The key whose value is searched for.
	 * @return The object associated with the field.
	 * @throws JSONException
	 *             If the field is not found.
	 */
	public Object get(String jsonText, String field) throws JSONException {
		JSONParser parser = new JSONParser(jsonText);

		return parser.parse(field).get("retValue");
	}

	/**
	 * Get the boolean value associated with a key form a JSON code. It can find
	 * embedded fields, too.
	 * 
	 * @param jsonText
	 *            JSON String in which the field is searched.
	 * @param field
	 *            The key whose value is searched for.
	 * @return The object associated with the field.
	 * @throws JSONException
	 *             If the field is not found.
	 */
	public boolean getBoolean(String jsonText, String field) throws JSONException {
		JSONParser parser = new JSONParser(jsonText);

		return parser.parse(field).getBoolean("retValue");
	}

	/**
	 * Get the double value associated with a key form a JSON code. It can find
	 * embedded fields, too.
	 * 
	 * @param jsonText
	 *            JSON String in which the field is searched.
	 * @param field
	 *            The key whose value is searched for.
	 * @return The object associated with the field.
	 * @throws JSONException
	 *             If the field is not found.
	 */
	public double getDouble(String jsonText, String field) throws JSONException {
		JSONParser parser = new JSONParser(jsonText);

		return parser.parse(field).getDouble("retValue");
	}

	/**
	 * Get the int value associated with a key form a JSON code. It can find
	 * embedded fields, too.
	 * 
	 * @param jsonText
	 *            JSON String in which the field is searched.
	 * @param field
	 *            The key whose value is searched for.
	 * @return The object associated with the field.
	 * @throws JSONException
	 *             If the field is not found.
	 */
	public int getInt(String jsonText, String field) throws JSONException {
		JSONParser parser = new JSONParser(jsonText);

		return parser.parse(field).getInt("retValue");
	}

	/**
	 * Get the long value associated with a key form a JSON code. It can find
	 * embedded fields, too.
	 * 
	 * @param jsonText
	 *            JSON String in which the field is searched.
	 * @param field
	 *            The key whose value is searched for.
	 * @return The object associated with the field.
	 * @throws JSONException
	 *             If the field is not found.
	 */
	public long getLong(String jsonText, String field) throws JSONException {
		JSONParser parser = new JSONParser(jsonText);

		return parser.parse(field).getLong("retValue");
	}
	
	/**
	 * Get the String value associated with a key form a JSON code. It can find
	 * embedded fields, too.
	 * 
	 * @param jsonText
	 *            JSON String in which the field is searched.
	 * @param field
	 *            The key whose value is searched for.
	 * @return The object associated with the field.
	 * @throws JSONException
	 *             If the field is not found.
	 */
	public String getString(String jsonText, String field) throws JSONException {
		JSONParser parser = new JSONParser(jsonText);
		
		return parser.parse(field).getString("retValue");
	}
}
