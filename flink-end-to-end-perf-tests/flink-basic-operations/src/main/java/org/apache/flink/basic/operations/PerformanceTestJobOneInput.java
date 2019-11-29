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

package org.apache.flink.basic.operations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end perf test for end-to-end perf test.
 *
 * <p>The sources are generated and bounded. The result is always constant.
 *
 * <p>Parameters:
 * --jobName job's name
 * --sourceParallelism  source Node's parallelism
 * --defaultParallelism
 * --checkPointMode  checkpointMode，atleastone or exactlyonce
 * --max generate random source
 * --seed
 * --wait
 * --wait_num
 * --windowSize
 * --rate
 * --sleepNum
 * --bigData
 * --streamPartitioner
 * --scheduleMode
 * --executionMode
 * --inputPath if source data in hdfs then input the inputPath
 * --checkpointInterval
 * --checkpointPath
 * --stateBackend
 * --checkpointTimeout
 */

public class PerformanceTestJobOneInput extends PerformanceTestJobBase{
	private static final Logger LOG = LoggerFactory.getLogger(PerformanceTestJobOneInput.class);

	public static void main(String[] args) throws Exception {
		PerformanceTestJobOneInput oneInput = new PerformanceTestJobOneInput();
		oneInput.initParams(args);
		oneInput.setEnv();
		oneInput.setGraph("oneInput");
		oneInput.setExecuteMode();
		oneInput.setScheduleMode();
		oneInput.setStateBackend();
		oneInput.run();
	}
}

