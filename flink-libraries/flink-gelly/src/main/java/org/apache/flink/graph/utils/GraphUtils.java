/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.utils;

import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.Utils.ChecksumHashCode;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.Graph;

public class GraphUtils {

	/**
	 * Computes the checksum over the Graph
	 *
	 * @return the checksum over the vertices and edges.
	 */
	public static Utils.ChecksumHashCode checksumHashCode(Graph graph) throws Exception {
		ChecksumHashCode checksum = DataSetUtils.checksumHashCode(graph.getVertices());
		checksum.add(DataSetUtils.checksumHashCode(graph.getEdges()));
		return checksum;
	}
}
