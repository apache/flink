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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;


/**
 * End-to-end perf test for end-to-end perf test.
 *
 * <p>The sources are generated and bounded. The result is always constant.
 *
 * <p>Parameters:
 * -sqlStatement SQL statement that will be executed as sqlUpdate
 * -jobName -sourceParallelism -defaultParallelism -outputPath -dataHubProjectName -checkPointMode -max -maxCount -seed
 * -wait -wait_num -windowSize -rate -sleepNum -bigData -streamPartitioner -scheduleMode -executionMode -inputPath
 * -checkpointInterval -checkpointPath -stateBackend -checkpointTimeout
 */
public class PerformanceTestJobTwoInputs extends PerformanceTestJobBase{

	public static void main(String[] args) throws Exception {
		PerformanceTestJobTwoInputs twoInputs = new PerformanceTestJobTwoInputs();
		twoInputs.initParams(args);
		twoInputs.setEnv();
		DataStream<Tuple2<String, String>> output1 = twoInputs.setGraph("oneInput1");
		DataStream<Tuple2<String, String>> output2 = twoInputs.setGraph("oneInput2");
		twoInputs.connector(output1, output2);
		twoInputs.setExecuteMode();
		twoInputs.setScheduleMode();
		twoInputs.setStateBackend();
		twoInputs.run();
	}
}
