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

package org.apache.flink.api.table.explain;

import java.util.List;

public class Node {
	private int id;
	private String type;
	private String pact;
	private String contents;
	private int parallelism;
	private String driver_strategy;
	private List<Predecessors> predecessors;
	private List<Global_properties> global_properties;
	private List<LocalProperty> local_properties;
	private List<Estimates> estimates;
	private List<Costs> costs;
	private List<Compiler_hints> compiler_hints;

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
	public String getDriver_strategy() {
		return driver_strategy;
	}
	public List<Predecessors> getPredecessors() {
		return predecessors;
	}
	public List<Global_properties> getGlobal_properties() {
		return global_properties;
	}
	public List<LocalProperty> getLocal_properties() {
		return local_properties;
	}
	public List<Estimates> getEstimates() {
		return estimates;
	}
	public List<Costs> getCosts() {
		return costs;
	}
	public List<Compiler_hints> getCompiler_hints() {
		return compiler_hints;
	}
}

class Predecessors {
	private String ship_strategy;
	private String exchange_mode;

	public String getShip_strategy() {
		return ship_strategy;
	}
	public String getExchange_mode() {
		return exchange_mode;
	}
}

class Global_properties {
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

class Compiler_hints {
	private String name;
	private String value;

	public String getValue() {
		return value;
	}
	public String getName() {
		return name;
	}
}
