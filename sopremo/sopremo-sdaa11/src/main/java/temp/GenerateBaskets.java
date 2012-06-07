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
package temp;

import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import eu.stratosphere.sopremo.sdaa11.frequent_itemsets.son.json.BasketNodes;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 * 
 */
public class GenerateBaskets {

	public static boolean ESCAPE_JSON = false;

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		final String[] vocabulary = "a b c d e f g h i j k l m n o".split(" ");
		final IArrayNode baskets = new ArrayNode();

		final int numBaskets = 1000;
		final int numItems = 3;

		final Set<Integer> chosenValueIndexes = new TreeSet<Integer>();
		IArrayNode itemsNode;
		ObjectNode basketNode;
		final Random random = new Random();

		for (int pointIndex = 0; pointIndex < numBaskets; pointIndex++) {
			chosenValueIndexes.clear();
			while (chosenValueIndexes.size() < numItems)
				chosenValueIndexes.add(random.nextInt(vocabulary.length));
			itemsNode = new ArrayNode();
			for (final int valueIndex : chosenValueIndexes)
				itemsNode.add(new TextNode(String
						.valueOf(vocabulary[valueIndex])));
			basketNode = new ObjectNode();
			BasketNodes.write(basketNode, itemsNode);
			baskets.add(basketNode);
		}

		String jsonString = baskets.toString();
		if (ESCAPE_JSON)
			jsonString = jsonString.replaceAll("\\\"", "\\\\\"");
		System.out.println(jsonString);
	}

}
