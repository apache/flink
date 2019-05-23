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

package org.apache.flink.table.explain;

import java.util.List;

/**
 * Field hierarchy of an execution plan.
 */
public class Node {
	private int id;
	private String type;
	private String pact;
	private String contents;
	private int parallelism;
	private String driverStrategy;
	private List<Predecessors> predecessors;
	private List<GlobalProperties> globalProperties;
	private List<LocalProperty> localProperties;
	private List<Estimates> estimates;
	private List<Costs> costs;
	private List<CompilerHints> compilerHints;

	public int getId() {
		return id;
	}

	public String getType() {
		return type;
	}

	public String getPact() {
		return pact;
	}

	public String getContents() {
		return contents;
	}

	public int getParallelism() {
		return parallelism;
	}

	public String getDriverStrategy() {
		return driverStrategy;
	}

	public List<Predecessors> getPredecessors() {
		return predecessors;
	}

	public List<GlobalProperties> getGlobalProperties() {
		return globalProperties;
	}

	public List<LocalProperty> getLocalProperties() {
		return localProperties;
	}

	public List<Estimates> getEstimates() {
		return estimates;
	}

	public List<Costs> getCosts() {
		return costs;
	}

	public List<CompilerHints> getCompilerHints() {
		return compilerHints;
	}
}

class Predecessors {
	private String shipStrategy;
	private String exchangeMode;

	public String getShipStrategy() {
		return shipStrategy;
	}

	public String getExchangeMode() {
		return exchangeMode;
	}
}

class GlobalProperties {
	private String name;
	private String value;

	public String getValue() {
		return value;
	}

	public String getName() {
		return name;
	}
}

class LocalProperty {
	private String name;
	private String value;

	public String getValue() {
		return value;
	}

	public String getName() {
		return name;
	}
}

class Estimates {
	private String name;
	private String value;

	public String getValue() {
		return value;
	}

	public String getName() {
		return name;
	}
}

class Costs {
	private String name;
	private String value;

	public String getValue() {
		return value;
	}

	public String getName() {
		return name;
	}
}

class CompilerHints {
	private String name;
	private String value;

	public String getValue() {
		return value;
	}

	public String getName() {
		return name;
	}
}
