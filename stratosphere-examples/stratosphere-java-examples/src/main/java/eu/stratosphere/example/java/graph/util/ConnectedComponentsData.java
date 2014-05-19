/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.example.java.graph.util;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.tuple.Tuple2;

/**
 * Provides the default data sets used for the Connected Components example program.
 * The default data sets are used, if no parameters are given to the program.
 *
 */
public class ConnectedComponentsData {

	public static DataSet<Long> getDefaultVertexDataSet(ExecutionEnvironment env) {
		
		return env.fromElements(
				1L, 2L, 3L, 4L, 5L, 
				6L, 7L, 8L, 9L, 10L,
				11L, 12L, 13L, 14L, 15L);
	}
	
	public static DataSet<Tuple2<Long, Long>> getDefaultEdgeDataSet(ExecutionEnvironment env) {
		
		List<Tuple2<Long, Long>> data = new ArrayList<Tuple2<Long, Long>>();
		data.add(new Tuple2<Long, Long>(1L, 2L));
		data.add(new Tuple2<Long, Long>(2L, 3L));
		data.add(new Tuple2<Long, Long>(2L, 4L));
		data.add(new Tuple2<Long, Long>(3L, 5L));
		data.add(new Tuple2<Long, Long>(6L, 7L));
		data.add(new Tuple2<Long, Long>(8L, 9L));
		data.add(new Tuple2<Long, Long>(8L, 10L));
		data.add(new Tuple2<Long, Long>(5L, 11L));
		data.add(new Tuple2<Long, Long>(11L, 12L));
		data.add(new Tuple2<Long, Long>(10L, 13L));
		data.add(new Tuple2<Long, Long>(9L, 14L));
		data.add(new Tuple2<Long, Long>(13L, 14L));
		data.add(new Tuple2<Long, Long>(1L, 15L));
		
		return env.fromCollection(data);
	}
	
}
