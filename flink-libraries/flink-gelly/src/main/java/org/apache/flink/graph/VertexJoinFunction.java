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

package org.apache.flink.graph;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.DataSet;

import java.io.Serializable;

/**
 * Interface to be implemented by the transformation function
 * applied in {@link Graph#joinWithVertices(DataSet, VertexJoinFunction)} method.
 *
 * @param <VV> the vertex value type
 * @param <T> the input value type
 */
public interface VertexJoinFunction<VV, T> extends Function, Serializable {

	/**
	 * Applies a transformation on the current vertex value
	 * and the value of the matched tuple of the input DataSet.
	 *
	 * @param vertexValue the current vertex value
	 * @param inputValue the value of the matched Tuple2 input
	 * @return the new vertex value
	 */
	VV vertexJoin(VV vertexValue, T inputValue) throws Exception;
}
