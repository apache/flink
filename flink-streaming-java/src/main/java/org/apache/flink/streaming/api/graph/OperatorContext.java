/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.StreamOperator;

import java.io.Serializable;

/**
 * Interface for {@link StreamOperator} to access context information. It is a view on partial context
 * of {@link OperatorConfig}
 */
@Internal
public interface OperatorContext {

	/**
	 * Returns the name of operator.
	 * @return The name of the operator.
	 */
	String getOperatorName();

	/**
	 * Returns ID of the operator node.
	 * @return ID of the operator node.
	 */
	int getNodeID();

	/**
	 * Returns whether the operator is chain start.
	 * @return Whether the operator is chain start.
	 */
	boolean isChainStart();

	/**
	 * Returns whether the operator is chain end.
	 * @return Whether the operator is chain end.
	 */
	boolean isChainEnd();

	/**
	 * Returns type serializer of the first input.
	 * @return Type serializer of the first input.
	 */
	<T> TypeSerializer<T> getTypeSerializerIn1();

	/**
	 * Returns type serializer of the second input.
	 * @return Type serializer of the second input.
	 */
	<T> TypeSerializer<T> getTypeSerializerIn2();

	/**
	 * Returns the state partitioner of the first input.
	 * @return The state partitioner of the first input.

	 */
	KeySelector<?, Serializable> getStatePartitioner1();

	/**
	 * Returns the state partitioner of the second input.
	 * @return The state partitioner of the second input.
	 */
	KeySelector<?, Serializable> getStatePartitioner2();

	/**
	 * Returns type serializer of state key.
	 * @return Type serializer of state key.
	 */
	<T> TypeSerializer<T> getStateKeySerializer();
}
