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

import eu.stratosphere.sopremo.sdaa11.clustering.json.PointNodes;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author skruse
 * 
 */
public class GeneratePoints {

	public static boolean ESCAPE_CHARS = false;

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		final String[] vocabulary = "a b c d e f g h i j k l m n o".split(" ");
		final IArrayNode points = new ArrayNode();

		final int numPoints = 100;
		final int numValues = 10;

		final Set<Integer> chosenValueIndexes = new TreeSet<Integer>();
		TextNode idNode;
		IArrayNode valuesNode;
		final IntNode rowsumNode = new IntNode(0);
		final Random random = new Random();

		for (int pointIndex = 0; pointIndex < numPoints; pointIndex++) {
			idNode = new TextNode(String.format("sample%03d", pointIndex));

			chosenValueIndexes.clear();
			while (chosenValueIndexes.size() < numValues)
				chosenValueIndexes.add(random.nextInt(vocabulary.length));
			valuesNode = new ArrayNode();
			for (final int valueIndex : chosenValueIndexes)
				valuesNode.add(new TextNode(String
						.valueOf(vocabulary[valueIndex])));

			final ObjectNode pointNode = new ObjectNode();
			PointNodes.write(pointNode, idNode, valuesNode, rowsumNode);
			points.add(pointNode);

		}

		String jsonString = points.toString();
		if (ESCAPE_CHARS)
			jsonString = jsonString.replaceAll("\\\"", "\\\\\"");
		System.out.println(jsonString);
	}

}
