/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.test.recordJobs.sort;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.test.recordJobs.sort.tsUtil.TeraDistribution;
import eu.stratosphere.test.recordJobs.sort.tsUtil.TeraInputFormat;
import eu.stratosphere.test.recordJobs.sort.tsUtil.TeraKey;
import eu.stratosphere.test.recordJobs.sort.tsUtil.TeraOutputFormat;

/**
 * This is an example implementation of the well-known TeraSort benchmark using the Stratosphere system. The benchmark
 * requires the input data to be generated according to the rules of Jim Gray's sort benchmark. A possible way to such
 * input data is the Hadoop TeraGen program. For more details see <a
 * href="http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/examples/terasort/TeraGen.html">
 * http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/examples/terasort/TeraGen.html</a>.
 *
 * Note: this example job is currently not included in the build, because of problems with the RangePartioner (see
 * https://github.com/stratosphere/stratosphere/issues/7). It should be included again after fixing the issue.
 *
 */
public final class TeraSort implements Program, ProgramDescription {

	private static final long serialVersionUID = 1L;


	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks] [input] [output]";
	}


	@Override
	public Plan getPlan(String... args) throws IllegalArgumentException {
		// parse job parameters
		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String input = (args.length > 1 ? args[1] : "");
		final String output = (args.length > 2 ? args[2] : "");

		// This task will read the input data and generate the key/value pairs
		final FileDataSource source = 
				new FileDataSource(new TeraInputFormat(), input, "Data Source");
		source.setDegreeOfParallelism(numSubTasks);

		// This task writes the sorted data back to disk
		final FileDataSink sink = 
				new FileDataSink(new TeraOutputFormat(), output, "Data Sink");
		sink.setDegreeOfParallelism(numSubTasks);
		sink.setGlobalOrder(new Ordering(0, TeraKey.class, Order.ASCENDING), new TeraDistribution());

		sink.setInput(source);

		return new Plan(sink, "TeraSort");
	}
}
