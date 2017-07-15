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

package org.apache.flink.graph.drivers.input;

import org.apache.flink.graph.drivers.transform.GraphKeyTypeTransform;
import org.apache.flink.graph.drivers.transform.Transform;
import org.apache.flink.graph.drivers.transform.Transformable;
import org.apache.flink.types.NullValue;

import java.util.Arrays;
import java.util.List;

/**
 * Base class for generated graphs.
 *
 * @param <K> graph ID type
 */
public abstract class GeneratedGraph<K>
extends InputBase<K, NullValue, NullValue>
implements Transformable {

	@Override
	public List<Transform> getTransformers() {
		return Arrays.<Transform>asList(new GraphKeyTypeTransform(vertexCount()));
	}

	/**
	 * The vertex count is verified to be no greater than the capacity of the
	 * selected data type. All vertices must be counted even if skipped or
	 * unused when generating graph edges.
	 *
	 * @return number of vertices configured for the graph
	 */
	protected abstract long vertexCount();
}
