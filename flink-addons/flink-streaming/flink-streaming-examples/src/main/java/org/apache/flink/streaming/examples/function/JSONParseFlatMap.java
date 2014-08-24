/**
 *
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
 *
 */

package org.apache.flink.streaming.examples.function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.RichFlatMapFunction;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

/**
 * Abstract class derived from {@link FlatMapFunction} to handle JSON files.
 * @param <IN>
 * Type of the input elements.
 * @param <OUT>
 * Type of the returned elements.
 */
public abstract class JSONParseFlatMap<IN, OUT> extends
		RichFlatMapFunction<IN, OUT> {

	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(JSONParseFlatMap.class);

	/**
	 * Get the value of a field in a JSON text.
	 * @param jsonText
	 * The JSON text in which the field is searched. 
	 * @param field
	 * The field which is searched in the JSON text.
	 * In case of embedded records fields have to be referred separated by dots.
	 * @return
	 * The value of the given field if it exists. Otherwise function returns with null.
	 */
	public String getField(String jsonText, String field) {
		JSONObject jo = null;
		try {
			jo = new JSONObject(jsonText);
		} catch (JSONException e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Input string is not proper", e);
			}
			return null;
		}

		try {
			String[] fieldArray = field.split("[.]");
			int length = fieldArray.length;

			return findInnerField(jo, fieldArray, length).getString(
					fieldArray[length - 1]);

		} catch (JSONException e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Field " + field + " not found");
			}
		}
		return null;
	}

	/**
	 * Find an embedded JSON code associated with the given key (fieldArray).
	 * @param jo
	 * JSONObject in which we search.
	 * @param fieldArray
	 * String array identifying the field.
	 * @param length
	 * Length of the array.
	 * @return
	 * the searched embedded JSONObject if it exists. 
	 * @throws JSONException
	 * if the key is not found.
	 */
	private JSONObject findInnerField(JSONObject jo, String[] fieldArray,
			int length) throws JSONException {

		for (int i = 0; i <= length - 2; i++) {
			jo = jo.getJSONObject(fieldArray[i]);
		}
		return jo;
	}
}
