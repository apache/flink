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
import org.apache.commons.lang3.text.WordUtils;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.drivers.output.CSV;
import org.apache.flink.graph.drivers.output.Print;
import org.apache.flink.graph.drivers.parameter.IterationConvergence;
import org.apache.flink.graph.library.link_analysis.HITS.Result;

/**
 * Driver for {@link org.apache.flink.graph.library.link_analysis.HITS}.
 */
public class HITS<K, VV, EV>
extends SimpleDriver<Result<K>>
implements Driver<K, VV, EV>, CSV, Print {

	private static final int DEFAULT_ITERATIONS = 10;

	private IterationConvergence iterationConvergence = new IterationConvergence(this, DEFAULT_ITERATIONS);

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}

	@Override
	public String getShortDescription() {
		return "score vertices as hubs and authorities";
	}

	@Override
	public String getLongDescription() {
		return WordUtils.wrap(new StrBuilder()
			.appendln("Hyperlink-Induced Topic Search computes two interdependent scores for " +
				"each vertex in a directed graph. A good \"hub\" links to good \"authorities\" " +
				"and good \"authorities\" are linked to from good \"hubs\".")
			.appendNewLine()
			.append("The result contains the vertex ID, hub score, and authority score.")
			.toString(), 80);
	}

	@Override
	public void plan(Graph<K, VV, EV> graph) throws Exception {
		result = graph
			.run(new org.apache.flink.graph.library.link_analysis.HITS<K, VV, EV>(
				iterationConvergence.getValue().iterations,
				iterationConvergence.getValue().convergenceThreshold));
	}
}
