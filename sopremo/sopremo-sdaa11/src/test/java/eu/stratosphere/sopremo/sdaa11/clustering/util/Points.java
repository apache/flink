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
package eu.stratosphere.sopremo.sdaa11.clustering.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.sdaa11.clustering.Point;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author skruse
 * 
 */
public class Points {

	volatile private static int pointCount = 0;

	public static final String POINTS1_PATH = "src/test/resources/clustering/points1";
	public static final String POINTS2_PATH = "src/test/resources/clustering/points1";

	public static IJsonNode asJson(final String... values) {
		return new Point(String.valueOf(pointCount++), values).write(null);
	}

	public static List<IJsonNode> loadPoints(final String filePath)
			throws IOException {
		BufferedReader reader = null;
		try {
			final File pointFile = new File(filePath);
			reader = new BufferedReader(new FileReader(pointFile));
			final JsonParser parser = new JsonParser(reader);
			final List<IJsonNode> pointNodes = new LinkedList<IJsonNode>();
			while (!parser.checkEnd())
				pointNodes.add(parser.readValueAsTree());
			return pointNodes;
		} finally {
			reader.close();
		}
	}

}
