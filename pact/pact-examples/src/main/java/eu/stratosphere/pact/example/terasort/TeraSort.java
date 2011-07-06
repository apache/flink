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

package eu.stratosphere.pact.example.terasort;

import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;

/**
 * This is an example implementation of the famous TeraSort benchmark using the Stratosphere system. The benchmark
 * requires the input data to be generated according to the rules of Jim Gray's sort benchmark. A possible way to such
 * input data is the Hadoop TeraGen program. For more details see <a
 * href="http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/examples/terasort/TeraGen.html">
 * http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/examples/terasort/TeraGen.html</a>.
 * 
 * @author warneke
 */
public final class TeraSort implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {

		return "Parameters: [noSubStasks] [input] [output]";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {

		// parse job parameters
		final int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String input = (args.length > 1 ? args[1] : "");
		final String output = (args.length > 2 ? args[2] : "");

		// This task will read the input data and generate the key/value pairs
		final FileDataSourceContract<TeraKey, TeraValue> source = new FileDataSourceContract<TeraKey, TeraValue>(
				TeraInputFormat.class, input, "Data Source");
		source.setDegreeOfParallelism(noSubTasks);

		// This stub task will do the actual sorting
		final ReduceContract<TeraKey, TeraValue, TeraKey, TeraValue> reduce = new ReduceContract<TeraKey, TeraValue, TeraKey, TeraValue>(
			TeraReduce.class);

		// This task writes the sorted data back to disk
		final FileDataSinkContract<TeraKey, TeraValue> sink = new FileDataSinkContract<TeraKey, TeraValue>(
			TeraOutputFormat.class, output, "Data Sink");
		sink.setDegreeOfParallelism(noSubTasks);

		reduce.setInput(source);
		sink.setInput(reduce);

		return new Plan(sink, "TeraSort");
	}

}
