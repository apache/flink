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

import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.source.ControllableSource;
import org.apache.flink.connectors.test.common.utils.FlinkContainers;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * Abstract Flink job for testing sink connector.
 *
 * <p>The topology of this job is:</p>
 *
 * <p>ControllableSource --> Testing sink</p>
 *
 * <p>The source controlled by testing framework will generate random records to downstream, which will be output
 * to external system by the sink connector. In the meantime, all generated random records will be written into
 * a plain text file named "record.txt" in the workspace managed by testing framework. </p>
 */
public abstract class AbstractSinkJob extends FlinkJob {

	/**
	 * Main entry of the job.
	 * @param externalContext Context of the test
	 * @throws Exception if job execution failed
	 */
	public void run(ExternalContext<String> externalContext) throws Exception {
		File recordFile = new File(FlinkContainers.getWorkspaceDirInside().getAbsolutePath(), "record.txt");
		ControllableSource controllableSource = new ControllableSource(recordFile.getAbsolutePath(), END_MARK);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.addSource(controllableSource).addSink(externalContext.createSink());
		env.execute(externalContext.jobName() + "-Sink");
	}

}
