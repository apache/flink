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

import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.StringTokenizer;

import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

/**
 * A JSONParser contains a JSONObject and provides opportunity to access
 * embedded fields in JSON code.
 */
public class JSONParser {

	private JSONObject originalJO;
	private String searchedfield;
	private Object temp;

	/**
	 * Construct a JSONParser from a string. The string has to be a JSON code
	 * from which we want to get a field.
	 * 
	 * @param jsonText
	 *            A string which contains a JSON code. String representation of
	 *            a JSON code.
	 * @throws JSONException
	 *             If there is a syntax error in the source string.
	 */
	public JSONParser(String jsonText) throws JSONException {
		originalJO = new JSONObject(jsonText);
	}

	/**
	 * 
	 * Parse the JSON code passed to the constructor to find the given key.
	 * 
	 * @param key
	 *            The key whose value is searched for.
	 * @return A JSONObject which has only one field called "retValue" and the
	 *         value associated to it is the searched value. The methods of
	 *         JSONObject can be used to get the field value in a desired
	 *         format.
	 * @throws JSONException
	 *             If the key is not found.
	 */
	public JSONObject parse(String key) throws JSONException {
		initializeParser(key);
		parsing();
		return putResultInJSONObj();
	}

	/**
	 * Prepare the fields of the class for the parsing
	 * 
	 * @param key
	 *            The key whose value is searched for.
	 * @throws JSONException
	 *             If the key is not found.
	 */
	private void initializeParser(String key) throws JSONException {
		searchedfield = key;
		temp = new JSONObject(originalJO.toString());
	}

	/**
	 * This function goes through the given field and calls the appropriate
	 * functions to treat the units between the punctuation marks.
	 * 
	 * @throws JSONException
	 *             If the key is not found.
	 */
	private void parsing() throws JSONException {
		StringTokenizer st = new StringTokenizer(searchedfield, ".");
		while (st.hasMoreTokens()) {
			find(st.nextToken());
		}
	}

	/**
	 * Search for the next part of the field and update the state if it was
	 * found.
	 * 
	 * @param nextToken
	 *            The current part of the searched field.
	 * @throws JSONException
	 *             If the key is not found.
	 */
	private void find(String nextToken) throws JSONException {
		if (endsWithBracket(nextToken)) {
			treatAllBracket(nextToken);
		} else {
			temp = ((JSONObject) temp).get(nextToken);
		}
	}

	/**
	 * Determine whether the given string ends with a closing square bracket ']'
	 * 
	 * @param nextToken
	 *            The current part of the searched field.
	 * @return True if the given string ends with a closing square bracket ']'
	 *         and false otherwise.
	 */
	private boolean endsWithBracket(String nextToken) {
		return nextToken.substring(nextToken.length() - 1).endsWith("]");
	}

	/**
	 * Handle (multidimensional) arrays. Treat the square bracket pairs one
	 * after the other if necessary.
	 * 
	 * @param nextToken
	 *            The current part of the searched field.
	 * @throws JSONException
	 *             If the searched element is not found.
	 */
	private void treatAllBracket(String nextToken) throws JSONException {
		List<String> list = Arrays.asList(nextToken.split("\\["));
		ListIterator<String> iter = list.listIterator();

		temp = ((JSONObject) temp).get(iter.next());

		while (iter.hasNext()) {
			int index = Integer.parseInt(cutBracket(iter.next()));
			temp = ((JSONArray) temp).get(index);
		}
	}

	/**
	 * Remove the last character of the string.
	 * 
	 * @param string
	 *            String to modify.
	 * @return The given string without the last character.
	 */
	private String cutBracket(String string) {
		return string.substring(0, string.length() - 1);
	}

	/**
	 * Save the result of the search into a JSONObject.
	 * 
	 * @return A special JSONObject which contain only one key. The value
	 *         associated to this key is the result of the search.
	 * @throws JSONException
	 *             If there is a problem creating the JSONObject. (e.g. invalid
	 *             syntax)
	 */
	private JSONObject putResultInJSONObj() throws JSONException {
		JSONObject jo = new JSONObject();
		jo.put("retValue", temp);
		return jo;
	}

}
