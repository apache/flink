/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io.channels.serialization;

import java.util.Random;

import eu.stratosphere.core.io.IOReadableWritable;

/**
 */
public interface SerializationTestType extends IOReadableWritable
{
	public SerializationTestType getRandom(Random rnd);

	// public static final String REST1_PATH = "src/test/resources/clustering/rest1.json";
	// public static final String SAMPLE1_PATH = "src/test/resources/clustering/sample1.json";
	//
	// public static IJsonNode asJson(final String... values) {
	// return new Point(String.valueOf(pointCount++), values).write(null);
	// }
	//
	// public static List<IJsonNode> loadPoints(final String filePath)
	// throws IOException {
	// BufferedReader reader = null;
	// try {
	// final File pointFile = new File(filePath);
	// reader = new BufferedReader(new FileReader(pointFile));
	// final JsonParser parser = new JsonParser(reader);
	// final List<IJsonNode> pointNodes = new LinkedList<IJsonNode>();
	// while (!parser.checkEnd())
	// pointNodes.add(parser.readValueAsTree());
	// return pointNodes;
	// } finally {
	// try {
	// reader.close();
	// } catch (final Exception e) {
	// e.printStackTrace();
	// }
	// }
	// }
}
