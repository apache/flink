/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.example.java.graph.util;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.example.java.graph.util.EnumTrianglesDataTypes.Edge;

/**
 * Provides the default data sets used for the Triangle Enumeration example programs.
 * The default data sets are used, if no parameters are given to the program.
 *
 */
public class EnumTrianglesData {

	public static DataSet<Edge> getDefaultEdgeDataSet(ExecutionEnvironment env) {
		
		return env.fromElements(new Edge(1, 2),
								new Edge(1, 3),
								new Edge(1, 4),
								new Edge(1, 5),
								new Edge(2, 3),
								new Edge(2, 5),
								new Edge(3, 4),
								new Edge(3, 7),
								new Edge(3, 8),
								new Edge(5, 6),
								new Edge(7, 8));
	}
}
