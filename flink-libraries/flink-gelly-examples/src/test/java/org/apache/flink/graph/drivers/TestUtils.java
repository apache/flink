/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.drivers;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Runner;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;

import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

/**
 * Utility methods for testing graph algorithm drivers.
 */
public class TestUtils {

	/**
	 * Verify algorithm driver parallelism.
	 *
	 * <p>Based on {@code org.apache.flink.graph.generator.TestUtils}.
	 *
	 * @param arguments program arguments
	 * @param fullParallelismOperatorNames list of regex strings matching the names of full parallelism operators
	 */
	static void verifyParallelism(String[] arguments, String... fullParallelismOperatorNames) throws Exception {
		// set a reduced parallelism for the algorithm runner
		final int parallelism = 8;
		arguments = ArrayUtils.addAll(arguments, "--__parallelism", Integer.toString(parallelism));

		// configure the runner but do not execute
		Runner runner = new Runner(arguments).run();

		// we cannot use the actual DataSink since DataSet#writeAsCsv also
		// executes the program; instead, we receive the DataSet and configure
		// with a DiscardingOutputFormat
		DataSet result = runner.getResult();
		if (result != null) {
			result.output(new DiscardingOutputFormat());
		}

		// set the default parallelism higher than the expected parallelism
		ExecutionEnvironment env = runner.getExecutionEnvironment();
		env.setParallelism(2 * parallelism);

		// add default regex exclusions for the added DiscardingOutputFormat
		// and also for any preceding GraphKeyTypeTransform
		List<Pattern> patterns = new ArrayList<>();
		patterns.add(Pattern.compile("DataSink \\(org\\.apache\\.flink\\.api\\.java\\.io\\.DiscardingOutputFormat@[0-9a-f]{1,8}\\)"));
		patterns.add(Pattern.compile("FlatMap \\(Translate results IDs\\)"));

		// add user regex patterns
		for (String largeOperatorName : fullParallelismOperatorNames) {
			patterns.add(Pattern.compile(largeOperatorName));
		}

		Optimizer compiler = new Optimizer(null, new DefaultCostEstimator(), new Configuration());
		OptimizedPlan optimizedPlan = compiler.compile(env.createProgramPlan());

		// walk the job plan from sinks to sources
		List<PlanNode> queue = new ArrayList<>();
		queue.addAll(optimizedPlan.getDataSinks());

		while (queue.size() > 0) {
			PlanNode node = queue.remove(queue.size() - 1);

			// skip operators matching an exclusion pattern; these are the
			// large-scale operators which run at full parallelism
			boolean matched = false;
			for (Pattern pattern : patterns) {
				matched |= pattern.matcher(node.getNodeName()).matches();
			}

			if (!matched) {
				// Data sources may have parallelism of 1, so simply check that the node
				// parallelism has not been increased by setting the default parallelism
				assertTrue("Wrong parallelism for " + node.toString(), node.getParallelism() <= parallelism);
			}

			for (Channel channel : node.getInputs()) {
				queue.add(channel.getSource());
			}
		}
	}
}
