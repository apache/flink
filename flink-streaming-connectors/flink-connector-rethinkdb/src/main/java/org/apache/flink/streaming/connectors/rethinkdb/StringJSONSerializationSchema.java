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

import org.json.simple.parser.JSONParser;

/**
 * String serialization schema to convert string to json object/array
 * 
 * @see RethinkDbSerializationSchema<T>
 * 
 * @see org.json.simple.JSONArray
 * @see org.json.simple.JSONObject
 */
public class StringJSONSerializationSchema implements JSONSerializationSchema<String>{

	/**
	 * Serial version of the class
	 */
	private static final long serialVersionUID = 686588590347479359L;

	/**
     * Convert String to JSON object
     * 
     * @param input string
     * @return JSONObject or JSONArray
     * 
     * @throws ParseException in case of problems parsing input string
     */
	@Override
	public Object toJSON(String input) throws Exception {
		JSONParser parser = new JSONParser();
		return parser.parse(input);
	}

}
