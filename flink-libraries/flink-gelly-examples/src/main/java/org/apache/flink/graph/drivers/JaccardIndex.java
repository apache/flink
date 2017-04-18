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
import org.apache.flink.graph.drivers.output.Hash;
import org.apache.flink.graph.drivers.output.Print;
import org.apache.flink.graph.drivers.parameter.LongParameter;
import org.apache.flink.graph.library.similarity.JaccardIndex.Result;
import org.apache.flink.types.CopyableValue;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Driver for {@link org.apache.flink.graph.library.similarity.JaccardIndex}.
 */
public class JaccardIndex<K extends CopyableValue<K>, VV, EV>
extends SimpleDriver<Result<K>>
implements Driver<K, VV, EV>, CSV, Hash, Print {

	private LongParameter littleParallelism = new LongParameter(this, "little_parallelism")
		.setDefaultValue(PARALLELISM_DEFAULT);

	@Override
	public String getName() {
		return this.getClass().getSimpleName();
	}

	@Override
	public String getShortDescription() {
		return "similarity score as fraction of common neighbors";
	}

	@Override
	public String getLongDescription() {
		return WordUtils.wrap(new StrBuilder()
			.appendln("Jaccard Index measures the similarity between vertex neighborhoods and " +
				"is computed as the number of shared neighbors divided by the number of " +
				"distinct neighbors. Scores range from 0.0 (no shared neighbors) to 1.0 (all " +
				"neighbors are shared).")
			.appendNewLine()
			.append("The result contains two vertex IDs, the number of shared neighbors, and " +
				"the number of distinct neighbors.")
			.toString(), 80);
	}

	@Override
	public void plan(Graph<K, VV, EV> graph) throws Exception {
		int lp = littleParallelism.getValue().intValue();

		result = graph
			.run(new org.apache.flink.graph.library.similarity.JaccardIndex<K, VV, EV>()
				.setLittleParallelism(lp));
	}
}
