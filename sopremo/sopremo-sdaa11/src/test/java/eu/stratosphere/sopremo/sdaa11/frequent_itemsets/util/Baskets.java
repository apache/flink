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
package eu.stratosphere.sopremo.sdaa11.frequent_itemsets.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 * 
 */
public class Baskets {

	public static final String BASKETS1_PATH = "src/test/resources/frequent_itemsets/baskets1";

	public static IJsonNode asJson(final String... values) {
		final IArrayNode result = new ArrayNode();
		for (final String value : values)
			result.add(new TextNode(value));
		return result;
	}

	public static List<IJsonNode> loadBaskets(final String filePath)
			throws IOException {
		BufferedReader reader = null;
		try {
			final File basketFile = new File(filePath);
			reader = new BufferedReader(new FileReader(basketFile));
			final JsonParser parser = new JsonParser(reader);
			final List<IJsonNode> basketNodes = new LinkedList<IJsonNode>();
			while (!parser.checkEnd())
				basketNodes.add(parser.readValueAsTree());
			return basketNodes;
		} finally {
			reader.close();
		}
	}

}
