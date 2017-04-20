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

import org.apache.commons.lang3.text.StrBuilder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.drivers.output.CSV;
import org.apache.flink.graph.drivers.output.Print;
import org.apache.flink.graph.drivers.parameter.DoubleParameter;
import org.apache.flink.graph.drivers.parameter.IterationConvergence;
import org.apache.flink.graph.library.link_analysis.PageRank.Result;

/**
 * @see org.apache.flink.graph.library.link_analysis.PageRank
 */
public class PageRank<K, VV, EV>
extends SimpleDriver<K, VV, EV, Result<K>>
implements CSV, Print {

	private static final int DEFAULT_ITERATIONS = 10;

	private DoubleParameter dampingFactor = new DoubleParameter(this, "damping_factor")
		.setDefaultValue(0.85)
		.setMinimumValue(0.0, false)
		.setMaximumValue(1.0, false);

	private IterationConvergence iterationConvergence = new IterationConvergence(this, DEFAULT_ITERATIONS);

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}

	@Override
	public String getShortDescription() {
		return "score vertices by the number and quality of incoming links";
	}

	@Override
	public String getLongDescription() {
		return new StrBuilder()
			.appendln("PageRank computes a per-vertex score which is the sum of PageRank scores " +
				"transmitted over in-edges. Each vertex's score is divided evenly among " +
				"out-edges. High-scoring vertices are linked to by other high-scoring vertices.")
			.appendNewLine()
			.append("The result contains the vertex ID and PageRank score.")
			.toString();
	}

	@Override
	protected DataSet<Result<K>> simplePlan(Graph<K, VV, EV> graph) throws Exception {
		return graph
			.run(new org.apache.flink.graph.library.link_analysis.PageRank<K, VV, EV>(
				dampingFactor.getValue(),
				iterationConvergence.getValue().iterations,
				iterationConvergence.getValue().convergenceThreshold));
	}
}
