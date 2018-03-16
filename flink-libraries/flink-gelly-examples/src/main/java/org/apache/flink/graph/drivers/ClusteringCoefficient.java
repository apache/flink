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

package org.apache.flink.graph.drivers;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAnalytic;
import org.apache.flink.graph.asm.result.PrintableResult;
import org.apache.flink.graph.drivers.parameter.ChoiceParameter;
import org.apache.flink.types.CopyableValue;

import org.apache.commons.lang3.text.StrBuilder;
import org.apache.commons.lang3.text.WordUtils;

import java.io.PrintStream;

/**
 * Driver for directed and undirected clustering coefficient algorithm and analytics.
 *
 * @see org.apache.flink.graph.library.clustering.directed.AverageClusteringCoefficient
 * @see org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient
 * @see org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient
 * @see org.apache.flink.graph.library.clustering.undirected.AverageClusteringCoefficient
 * @see org.apache.flink.graph.library.clustering.undirected.GlobalClusteringCoefficient
 * @see org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient
 */
public class ClusteringCoefficient<K extends Comparable<K> & CopyableValue<K>, VV, EV>
extends DriverBase<K, VV, EV> {

	private static final String DIRECTED = "directed";

	private static final String UNDIRECTED = "undirected";

	private ChoiceParameter order = new ChoiceParameter(this, "order")
		.addChoices(DIRECTED, UNDIRECTED);

	private GraphAnalytic<K, VV, EV, ? extends PrintableResult> globalClusteringCoefficient;

	private GraphAnalytic<K, VV, EV, ? extends PrintableResult> averageClusteringCoefficient;

	@Override
	public String getShortDescription() {
		return "measure the connectedness of vertex neighborhoods";
	}

	@Override
	public String getLongDescription() {
		return WordUtils.wrap(new StrBuilder()
			.appendln("The local clustering coefficient measures the connectedness of each " +
				"vertex's neighborhood. The global clustering coefficient measures the " +
				"connected of the graph. The average clustering coefficient is the mean local " +
				"clustering coefficient. Each score ranges from 0.0 (no edges between vertex " +
				"neighbors) to 1.0 (neighborhood or graph is a clique).")
			.appendNewLine()
			.append("The algorithm result contains the vertex ID, degree, and number of edges " +
				"connecting neighbors.")
			.toString(), 80);
	}

	@Override
	public DataSet plan(Graph<K, VV, EV> graph) throws Exception {
		int parallelism = this.parallelism.getValue().intValue();

		switch (order.getValue()) {
			case DIRECTED:
				globalClusteringCoefficient = graph
					.run(new org.apache.flink.graph.library.clustering.directed.GlobalClusteringCoefficient<K, VV, EV>()
						.setParallelism(parallelism));

				averageClusteringCoefficient = graph
					.run(new org.apache.flink.graph.library.clustering.directed.AverageClusteringCoefficient<K, VV, EV>()
						.setParallelism(parallelism));

				@SuppressWarnings("unchecked")
				DataSet<PrintableResult> directedResult = (DataSet<PrintableResult>) (DataSet<?>) graph
					.run(new org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient<K, VV, EV>()
						.setParallelism(parallelism));
				return directedResult;

			case UNDIRECTED:
				globalClusteringCoefficient = graph
					.run(new org.apache.flink.graph.library.clustering.undirected.GlobalClusteringCoefficient<K, VV, EV>()
						.setParallelism(parallelism));

				averageClusteringCoefficient = graph
					.run(new org.apache.flink.graph.library.clustering.undirected.AverageClusteringCoefficient<K, VV, EV>()
						.setParallelism(parallelism));

				@SuppressWarnings("unchecked")
				DataSet<PrintableResult> undirectedResult = (DataSet<PrintableResult>) (DataSet<?>) graph
					.run(new org.apache.flink.graph.library.clustering.undirected.LocalClusteringCoefficient<K, VV, EV>()
						.setParallelism(parallelism));
				return undirectedResult;

			default:
				throw new RuntimeException("Unknown order: " + order);
		}
	}

	@Override
	public void printAnalytics(PrintStream out) {
		out.println(globalClusteringCoefficient.getResult().toPrintableString());
		out.println(averageClusteringCoefficient.getResult().toPrintableString());
	}
}
