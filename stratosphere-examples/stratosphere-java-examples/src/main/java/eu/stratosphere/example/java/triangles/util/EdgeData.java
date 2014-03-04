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
package eu.stratosphere.example.java.triangles.util;

import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.example.java.triangles.util.EdgeDataTypes.Edge;

public class EdgeData {

	public static DataSet<Edge> getDefaultEdgeDataSet(ExecutionEnvironment env) {
		
		Set<Edge> edgeData = new HashSet<Edge>();
		
		edgeData.add(new Edge(1, 2));
		edgeData.add(new Edge(1, 3));
		edgeData.add(new Edge(1, 4));
		edgeData.add(new Edge(1, 5));
		edgeData.add(new Edge(2, 3));
		edgeData.add(new Edge(2, 5));
		edgeData.add(new Edge(3, 4));
		edgeData.add(new Edge(3, 7));
		edgeData.add(new Edge(3, 8));
		edgeData.add(new Edge(5, 6));
		edgeData.add(new Edge(7, 8));
				
		TupleTypeInfo<Edge> edgeType = 
				new TupleTypeInfo<Edge>(
						Edge.class,
						BasicTypeInfo.INT_TYPE_INFO,
						BasicTypeInfo.INT_TYPE_INFO);
		
		return env.fromCollection(edgeData, edgeType);
		
	}
	
}
