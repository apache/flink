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

package org.apache.flink.connectors.test.common.jobs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.external.SourceJobTerminationPattern;
import org.apache.flink.connectors.test.common.sink.SimpleFileSink;
import org.apache.flink.connectors.test.common.utils.FlinkContainers;
import org.apache.flink.connectors.test.common.utils.SuccessException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * Abstract Flink job for testing source connector.
 *
 * <p>The topology of this job is: </p>
 *
 * <p>Tested source --> map(optional) -> SimpleFileSink</p>
 *
 * <p>The source will consume records from external system and send them to downstream via an optional map operator,
 * and records will be written into a file named "output.txt" in the workspace managed by testing framework.</p>
 *
 * <p>If the job termination pattern is {@link SourceJobTerminationPattern#END_MARK_FILTERING}, which means
 * the tested source is unbounded, a map operator will be inserted between source and sink for filtering the end mark,
 * and a {@link SuccessException} will be thrown for terminating the job if the end mark is received by the map
 * operator. </p>
 */
public abstract class AbstractSourceJob extends FlinkJob {

	/**
	 * Main entry of the job.
	 * @param context Context of the test
	 * @throws Exception if job execution failed
	 */
	public void run(ExternalContext<String> context) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		if (context.sourceJobTerminationPattern() == SourceJobTerminationPattern.END_MARK_FILTERING) {
			env.setRestartStrategy(RestartStrategies.noRestart());
		}

		File outputFile = new File(FlinkContainers.getWorkspaceDirInside().getAbsolutePath(), "output.txt");

		DataStream<String> stream = env.addSource(context.createSource());

		switch (context.sourceJobTerminationPattern()) {
			case END_MARK_FILTERING:
				stream = stream.map((MapFunction<String, String>) value -> {
					if (value.equals(END_MARK)) {
						throw new SuccessException("Successfully received end mark");
					}
					return value;
				});
				break;
			case BOUNDED_SOURCE:
			case DESERIALIZATION_SCHEMA:
			case FORCE_STOP:
				break;
			default:
				throw new IllegalStateException("Unrecognized stop pattern");
		}
		stream.addSink(new SimpleFileSink(outputFile.getAbsolutePath(), false));
		env.execute(context.jobName() + "-Source");
	}
}
