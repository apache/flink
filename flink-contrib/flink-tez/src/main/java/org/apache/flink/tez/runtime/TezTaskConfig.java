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

package org.apache.flink.tez.runtime;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class TezTaskConfig extends TaskConfig {

	public static final String TEZ_TASK_CONFIG = "tez.task.flink.processor.taskconfig";

	private static final String NUMBER_SUBTASKS_IN_OUTPUTS = "tez.num_subtasks_in_output";

	private static final String INPUT_SPLIT_PROVIDER = "tez.input_split_provider";

	private static final String INPUT_POSITIONS = "tez.input_positions";

	private static final String INPUT_FORMAT = "tez.input_format";

	private static final String DATASOURCE_PROCESSOR_NAME = "tez.datasource_processor_name";

	public TezTaskConfig(Configuration config) {
		super(config);
	}


	public void setDatasourceProcessorName(String name) {
		if (name != null) {
			this.config.setString(DATASOURCE_PROCESSOR_NAME, name);
		}
	}

	public String getDatasourceProcessorName() {
		return this.config.getString(DATASOURCE_PROCESSOR_NAME, null);
	}

	public void setNumberSubtasksInOutput(ArrayList<Integer> numberSubtasksInOutputs) {
		try {
			InstantiationUtil.writeObjectToConfig(numberSubtasksInOutputs, this.config, NUMBER_SUBTASKS_IN_OUTPUTS);
		} catch (IOException e) {
			throw new RuntimeException("Error while writing the input split provider object to the task configuration.");
		}
	}

	public ArrayList<Integer> getNumberSubtasksInOutput() {
		ArrayList<Integer> numberOfSubTasksInOutputs = null;
		try {
			numberOfSubTasksInOutputs = (ArrayList) InstantiationUtil.readObjectFromConfig(this.config, NUMBER_SUBTASKS_IN_OUTPUTS, getClass().getClassLoader());
		}
		catch (IOException e) {
			throw new RuntimeException("Error while reading the number of subtasks in outputs object from the task configuration.");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error while reading the number of subtasks in outpurs object from the task configuration. " +
					"class not found.");
		}
		if (numberOfSubTasksInOutputs == null) {
			throw new NullPointerException();
		}
		return numberOfSubTasksInOutputs;

	}


	public void setInputSplitProvider (InputSplitProvider inputSplitProvider) {
		try {
			InstantiationUtil.writeObjectToConfig(inputSplitProvider, this.config, INPUT_SPLIT_PROVIDER);
		} catch (IOException e) {
			throw new RuntimeException("Error while writing the input split provider object to the task configuration.");
		}
	}

	public InputSplitProvider getInputSplitProvider () {
		InputSplitProvider inputSplitProvider = null;
		try {
			inputSplitProvider = (InputSplitProvider) InstantiationUtil.readObjectFromConfig(this.config, INPUT_SPLIT_PROVIDER, getClass().getClassLoader());
		}
		catch (IOException e) {
			throw new RuntimeException("Error while reading the input split provider object from the task configuration.");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error while reading the input split provider object from the task configuration. " +
					"ChannelSelector class not found.");
		}
		if (inputSplitProvider == null) {
			throw new NullPointerException();
		}
		return inputSplitProvider;
	}


	public void setInputPositions(HashMap<String,ArrayList<Integer>> inputPositions) {
		try {
			InstantiationUtil.writeObjectToConfig(inputPositions, this.config, INPUT_POSITIONS);
		} catch (IOException e) {
			throw new RuntimeException("Error while writing the input positions object to the task configuration.");
		}
	}

	public HashMap<String,ArrayList<Integer>> getInputPositions () {
		HashMap<String,ArrayList<Integer>> inputPositions = null;
		try {
			inputPositions = (HashMap) InstantiationUtil.readObjectFromConfig(this.config, INPUT_POSITIONS, getClass().getClassLoader());
		}
		catch (IOException e) {
			throw new RuntimeException("Error while reading the input positions object from the task configuration.");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error while reading the input positions object from the task configuration. " +
					"ChannelSelector class not found.");
		}
		if (inputPositions == null) {
			throw new NullPointerException();
		}
		return inputPositions;
	}

	public void setInputFormat (InputFormat inputFormat) {
		try {
			InstantiationUtil.writeObjectToConfig(inputFormat, this.config, INPUT_FORMAT);
		} catch (IOException e) {
			throw new RuntimeException("Error while writing the input format object to the task configuration.");
		}
	}

	public InputFormat getInputFormat () {
		InputFormat inputFormat = null;
		try {
			inputFormat = (InputFormat) InstantiationUtil.readObjectFromConfig(this.config, INPUT_FORMAT, getClass().getClassLoader());
		}
		catch (IOException e) {
			throw new RuntimeException("Error while reading the input split provider object from the task configuration.");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error while reading the input split provider object from the task configuration. " +
					"ChannelSelector class not found.");
		}
		if (inputFormat == null) {
			throw new NullPointerException();
		}
		return inputFormat;
	}

}
