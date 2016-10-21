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

package org.apache.flink.graph;

/**
 * This default main class prints usage listing available classes.
 */
public class Usage {

	private static final Class[] DRIVERS = new Class[]{
		org.apache.flink.graph.drivers.ClusteringCoefficient.class,
		org.apache.flink.graph.drivers.Graph500.class,
		org.apache.flink.graph.drivers.GraphMetrics.class,
		org.apache.flink.graph.drivers.HITS.class,
		org.apache.flink.graph.drivers.JaccardIndex.class,
		org.apache.flink.graph.drivers.TriangleListing.class,
	};

	private static final Class[] EXAMPLES = new Class[]{
		org.apache.flink.graph.examples.ConnectedComponents.class,
		org.apache.flink.graph.examples.EuclideanGraphWeighing.class,
		org.apache.flink.graph.examples.GSASingleSourceShortestPaths.class,
		org.apache.flink.graph.examples.IncrementalSSSP.class,
		org.apache.flink.graph.examples.MusicProfiles.class,
		org.apache.flink.graph.examples.PregelSSSP.class,
		org.apache.flink.graph.examples.SingleSourceShortestPaths.class,
		org.apache.flink.graph.scala.examples.ConnectedComponents.class,
		org.apache.flink.graph.scala.examples.GSASingleSourceShortestPaths.class,
		org.apache.flink.graph.scala.examples.SingleSourceShortestPaths.class,
	};

	public static void main(String[] args) throws Exception {
		System.out.println("Driver classes call algorithms from the Gelly library:");
		for (Class cls : DRIVERS) {
			System.out.println("  " + cls.getName());
		}

		System.out.println("");
		System.out.println("Example classes illustrate Gelly APIs or alternative algorithms:");
		for (Class cls : EXAMPLES) {
			System.out.println("  " + cls.getName());
		}
	}
}
