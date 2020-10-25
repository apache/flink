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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

/**
 * Use cases of managed memory.
 */
@Internal
public enum ManagedMemoryUseCase {
	BATCH_OP(Scope.OPERATOR),
	STATE_BACKEND(Scope.SLOT),
	PYTHON(Scope.SLOT);

	public final Scope scope;

	ManagedMemoryUseCase(Scope scope) {
		this.scope = Preconditions.checkNotNull(scope);
	}

	/**
	 * Scope at which memory is managed for a use case.
	 */
	public enum Scope {
		SLOT,
		OPERATOR
	}
}
