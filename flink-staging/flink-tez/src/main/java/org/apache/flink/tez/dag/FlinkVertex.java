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

package org.apache.flink.tez.dag;


import org.apache.flink.tez.runtime.TezTaskConfig;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public abstract class FlinkVertex {

	protected Vertex cached;
	private String taskName;
	private int parallelism;
	protected TezTaskConfig taskConfig;

	// Tez-specific bookkeeping
	protected String uniqueName; //Unique name in DAG
	private Map<FlinkVertex,ArrayList<Integer>> inputPositions;
	private ArrayList<Integer> numberOfSubTasksInOutputs;

	public TezTaskConfig getConfig() {
		return taskConfig;
	}

	public FlinkVertex(String taskName, int parallelism, TezTaskConfig taskConfig) {
		this.cached = null;
		this.taskName = taskName;
		this.parallelism = parallelism;
		this.taskConfig = taskConfig;
		this.uniqueName = taskName + UUID.randomUUID().toString();
		this.inputPositions = new HashMap<FlinkVertex, ArrayList<Integer>>();
		this.numberOfSubTasksInOutputs = new ArrayList<Integer>();
	}

	public int getParallelism () {
		return parallelism;
	}

	public void setParallelism (int parallelism) {
		this.parallelism = parallelism;
	}

	public abstract Vertex createVertex (TezConfiguration conf);

	public Vertex getVertex () {
		return cached;
	}

	protected String getUniqueName () {
		return uniqueName;
	}

	public void addInput (FlinkVertex vertex, int position) {
		if (inputPositions.containsKey(vertex)) {
			inputPositions.get(vertex).add(position);
		}
		else {
			ArrayList<Integer> lst = new ArrayList<Integer>();
			lst.add(position);
			inputPositions.put(vertex,lst);
		}
	}

	public void addNumberOfSubTasksInOutput (int subTasks, int position) {
		if (numberOfSubTasksInOutputs.isEmpty()) {
			numberOfSubTasksInOutputs.add(-1);
		}
		int currSize = numberOfSubTasksInOutputs.size();
		for (int i = currSize; i <= position; i++) {
			numberOfSubTasksInOutputs.add(i, -1);
		}
		numberOfSubTasksInOutputs.set(position, subTasks);
	}

	// Must be called before taskConfig is written to Tez configuration
	protected void writeInputPositionsToConfig () {
		HashMap<String,ArrayList<Integer>> toWrite = new HashMap<String, ArrayList<Integer>>();
		for (FlinkVertex v: inputPositions.keySet()) {
			String name = v.getUniqueName();
			List<Integer> positions = inputPositions.get(v);
			toWrite.put(name, new ArrayList<Integer>(positions));
		}
		this.taskConfig.setInputPositions(toWrite);
	}

	// Must be called before taskConfig is written to Tez configuration
	protected void writeSubTasksInOutputToConfig () {
		this.taskConfig.setNumberSubtasksInOutput(this.numberOfSubTasksInOutputs);
	}

}
