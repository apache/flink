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

package org.apache.flink.streaming.runtime.partitioner;

/**
 * Interface for {@link StreamPartitioner} which have to be configured with the maximum parallelism
 * of the stream transformation. The configure method is called by the StreamGraph when adding
 * internal edges.
 *
 * <p>This interface is required since the stream partitioners are instantiated eagerly. Due to that
 * the maximum parallelism might not have been determined and needs to be set at a stage when the
 * maximum parallelism could have been determined.
 */
public interface ConfigurableStreamPartitioner {

	/**
	 * Configure the {@link StreamPartitioner} with the maximum parallelism of the down stream
	 * operator.
	 *
	 * @param maxParallelism Maximum parallelism of the down stream operator.
	 */
	void configure(int maxParallelism);
}
