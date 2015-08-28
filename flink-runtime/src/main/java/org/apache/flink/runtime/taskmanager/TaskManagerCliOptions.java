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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.StreamingMode;

/**
 * The command line parameters passed to the TaskManager.
 */
public class TaskManagerCliOptions {

	private String configDir;
	
	private StreamingMode mode = StreamingMode.BATCH_ONLY;
	
	// ------------------------------------------------------------------------

	public String getConfigDir() {
		return configDir;
	}

	public void setConfigDir(String configDir) {
		this.configDir = configDir;
	}

	public StreamingMode getMode() {
		return mode;
	}

	public void setMode(String modeName) {
		if (modeName.equalsIgnoreCase("streaming")) {
			this.mode = StreamingMode.STREAMING;
		}
		else if (modeName.equalsIgnoreCase("batch")) {
			this.mode = StreamingMode.BATCH_ONLY;
		}
		else {
			throw new IllegalArgumentException("Mode must be one of 'BATCH' or 'STREAMING'.");
		}
	}
}