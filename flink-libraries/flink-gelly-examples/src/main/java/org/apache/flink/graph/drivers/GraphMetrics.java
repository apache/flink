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

import org.apache.commons.lang3.text.StrBuilder;

import java.io.PrintStream;

/**
 * Driver for directed and undirected graph metrics analytics.
 *
 * @see org.apache.flink.graph.library.metric.directed.EdgeMetrics
 * @see org.apache.flink.graph.library.metric.directed.VertexMetrics
 * @see org.apache.flink.graph.library.metric.undirected.EdgeMetrics
 * @see org.apache.flink.graph.library.metric.undirected.VertexMetrics
 */
public class GraphMetrics<K extends Comparable<K>, VV, EV>
extends DriverBase<K, VV, EV> {

	private static final String DIRECTED = "directed";

	private static final String UNDIRECTED = "undirected";

	private ChoiceParameter order = new ChoiceParameter(this, "order")
		.addChoices(DIRECTED, UNDIRECTED);

	private GraphAnalytic<K, VV, EV, ? extends PrintableResult> vertexMetrics;

	private GraphAnalytic<K, VV, EV, ? extends PrintableResult> edgeMetrics;

	@Override
	public String getShortDescription() {
		return "compute vertex and edge metrics";
	}

	@Override
	public String getLongDescription() {
		return new StrBuilder()
			.appendln("Computes metrics on a directed or undirected graph.")
			.appendNewLine()
			.appendln("Vertex metrics:")
			.appendln("- number of vertices")
			.appendln("- number of edges")
			.appendln("- number of unidirectional edges (directed only)")
			.appendln("- number of bidirectional edges (directed only)")
			.appendln("- average degree")
			.appendln("- number of triplets")
			.appendln("- maximum degree")
			.appendln("- maximum out degree (directed only)")
			.appendln("- maximum in degree (directed only)")
			.appendln("- maximum number of triplets")
			.appendNewLine()
			.appendln("Edge metrics:")
			.appendln("- number of triangle triplets")
			.appendln("- number of rectangle triplets")
			.appendln("- maximum number of triangle triplets")
			.append("- maximum number of rectangle triplets")
			.toString();
	}

	@Override
	public DataSet plan(Graph<K, VV, EV> graph) throws Exception {
		switch (order.getValue()) {
			case DIRECTED:
				vertexMetrics = graph
					.run(new org.apache.flink.graph.library.metric.directed.VertexMetrics<K, VV, EV>()
						.setParallelism(parallelism.getValue().intValue()));

				edgeMetrics = graph
					.run(new org.apache.flink.graph.library.metric.directed.EdgeMetrics<K, VV, EV>()
						.setParallelism(parallelism.getValue().intValue()));
				break;

			case UNDIRECTED:
				vertexMetrics = graph
					.run(new org.apache.flink.graph.library.metric.undirected.VertexMetrics<K, VV, EV>()
						.setParallelism(parallelism.getValue().intValue()));

				edgeMetrics = graph
					.run(new org.apache.flink.graph.library.metric.undirected.EdgeMetrics<K, VV, EV>()
						.setParallelism(parallelism.getValue().intValue()));
				break;
		}

		return null;
	}

	@Override
	public void printAnalytics(PrintStream out) {
		out.print("Vertex metrics:\n  ");
		out.println(vertexMetrics.getResult().toPrintableString().replace(";", "\n "));

		out.println();
		out.print("Edge metrics:\n  ");
		out.println(edgeMetrics.getResult().toPrintableString().replace(";", "\n "));
	}
}
