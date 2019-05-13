/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

/**
 * A connection between {@link FailoverVertex FailoverVertices}.
 *
 * <p>producer -> ResultPartition -> consumer
 */
public interface FailoverEdge {

	/**
	 * Returns the ID of the result partition that the source produces.
	 *
	 * @return ID of the result partition that the source produces
	 */
	IntermediateResultPartitionID getResultPartitionID();

	/**
	 * Returns the {@link ResultPartitionType} of the produced result partition.
	 *
	 * @return type of the produced result partition
	 */
	ResultPartitionType getResultPartitionType();

	/**
	 * Returns the source vertex, i.e., the producer of the result partition.
	 *
	 * @return source vertex
	 */
	FailoverVertex getSourceVertex();

	/**
	 * Returns the target vertex, i.e., the consumer of the result partition.
	 *
	 * @return target vertex
	 */
	FailoverVertex getTargetVertex();
}
