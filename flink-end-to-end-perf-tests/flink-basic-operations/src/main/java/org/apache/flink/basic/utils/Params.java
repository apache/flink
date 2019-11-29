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

package org.apache.flink.basic.utils;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Provides the default data sets used for the WordCount example program. The default data sets are used, if no
 * parameters are given to the program.
 */

public class Params {
	String jobName;
	int parallelism;
	String outputPath;
	String checkPointMode;
	int max;
	Long maxCount;
	int seed;
	int waitNum;
	long windowSize;
	long rate;
	int sleepNum;
	boolean bigData;
	String streamPartitioner;
	String scheduleMode;
	String executionMode;
	long checkpointInterval;
	String checkpointPath;
	String stateBackend;
	long checkpointTimeout;
	int recordSize;

	public void initEnv(ParameterTool params) {
		this.jobName = params.get("jobName", "Basic PerformanceTestJob");
		this.parallelism = params.getInt("parallelism", 1);
		this.outputPath = params.get("outputPath", "");
		this.checkPointMode = params.get("checkpointMode", "AtLeastOnce");
		this.max = params.getInt("max", 100);
		this.maxCount = params.getLong("maxCount", Long.MAX_VALUE);
		this.seed = params.getInt("seed", 0);
		this.waitNum = params.getInt("wait_num", 1000);
		this.windowSize = params.getLong("windowSize", 2000);
		this.sleepNum = params.getInt("sleepNum", 0);
		this.bigData = params.getBoolean("bigData", true);
		this.streamPartitioner = params.get("streamPartitioner", "KeyBy");
		this.scheduleMode = params.get("scheduleMode", "EAGER");
		this.executionMode = params.get("executionMode", "PIPELINED");
		this.checkpointInterval = params.getLong("checkpointInterval", 0);
		this.checkpointPath = params.get("checkpointPath", "/");
		this.stateBackend = params.get("stateBackend", "heap");
		this.checkpointTimeout = params.getLong("checkpointTimeout", 15 * 1000);
		this.recordSize = params.getInt("recordSize", 1024);
	}

	public String getJobName() {
		return jobName;
	}

	public int getParallelism() {
		return parallelism;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public String getCheckPointMode() {
		return checkPointMode;
	}

	public Long getMaxCount() {
		return maxCount;
	}

	public int getSleepNum() {
		return sleepNum;
	}

	public String getStreamPartitioner() {
		return streamPartitioner;
	}

	public String getScheduleMode() {
		return scheduleMode;
	}

	public String getExecutionMode() {
		return executionMode;
	}

	public long getCheckpointInterval() {
		return checkpointInterval;
	}

	public String getCheckpointPath() {
		return checkpointPath;
	}

	public String getStateBackend() {
		return stateBackend;
	}

	public long getCheckpointTimeout() {
		return checkpointTimeout;
	}

	public int getRecordSize(){
		return recordSize;
	}

	public int getSeed() {
		return seed;
	}

	public int getWaitNum() {
		return waitNum;
	}
}

