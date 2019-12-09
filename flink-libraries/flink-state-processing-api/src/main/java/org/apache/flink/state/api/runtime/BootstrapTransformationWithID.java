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

package org.apache.flink.state.api.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.util.Preconditions;

/**
 * A simple container class that represents a newly bootstrapped operator state within savepoints.
 * It wraps the target {@link OperatorID} for the bootstrapped operator, as well as the {@link BootstrapTransformation}
 * that defines how the state is bootstrapped.
 */
@Internal
public class BootstrapTransformationWithID<T> {

	private final OperatorID operatorID;
	private final BootstrapTransformation<T> bootstrapTransformation;

	public BootstrapTransformationWithID(OperatorID operatorID, BootstrapTransformation<T> bootstrapTransformation) {
		this.operatorID = Preconditions.checkNotNull(operatorID);
		this.bootstrapTransformation = Preconditions.checkNotNull(bootstrapTransformation);
	}

	public OperatorID getOperatorID() {
		return operatorID;
	}

	public BootstrapTransformation<T> getBootstrapTransformation() {
		return bootstrapTransformation;
	}
}
