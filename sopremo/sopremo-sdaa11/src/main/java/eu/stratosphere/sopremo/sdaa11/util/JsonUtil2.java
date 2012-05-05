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
package eu.stratosphere.sopremo.sdaa11.util;

import eu.stratosphere.sopremo.sdaa11.JsonSerializable;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 * 
 */
public class JsonUtil2 {

	public static ObjectNode reuseObjectNode(final IJsonNode node) {
		if (node == null || !(node instanceof ObjectNode))
			return new ObjectNode();
		return (ObjectNode) node;
	}

	public static boolean isObjectNode(final IJsonNode node) {
		return node != null && node instanceof ObjectNode;
	}

	@SuppressWarnings("unchecked")
	public static <T> T getField(final IJsonNode node, final String field,
			final Class<T> type) {
		return (T) ((ObjectNode) node).get(field);
	}

	public static IJsonNode getField(final IJsonNode node, final String field) {
		return ((ObjectNode) node).get(field);
	}
	
	public static void copyStrings(IArrayNode array, Iterable<String> values) {
		array.clear();
		for (String value : values) {
			array.add(new TextNode(value));
		}
	}
	
	public static void copy(IArrayNode array, Iterable<JsonSerializable> values) {
		array.clear();
		for (JsonSerializable value : values) {
			array.add(value.write(null));
		}
	}

}
