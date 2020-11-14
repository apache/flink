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

package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * An interface for partitioning records.
 *
 * @param <T> record type
 */
@PublicEvolving
public abstract class KinesisPartitioner<T> implements Serializable {

	private static final long serialVersionUID = -7467294664702189780L;

	/**
	 * Return a partition id based on the input.
	 *
	 * @param element Element to partition
	 *
	 * @return A string representing the partition id
	 */
	public abstract String getPartitionId(T element);

	/**
	 * Optional method for setting an explicit hash key.
	 *
	 * @param element Element to get the hash key for
	 *
	 * @return the hash key for the element
	 */
	public String getExplicitHashKey(T element) {
		return null;
	}

	/**
	 * Optional initializer.
	 *
	 * @param indexOfThisSubtask Index of this partitioner instance
	 * @param numberOfParallelSubtasks Total number of parallel instances
	 */
	public void initialize(int indexOfThisSubtask, int numberOfParallelSubtasks) {
	}
}
