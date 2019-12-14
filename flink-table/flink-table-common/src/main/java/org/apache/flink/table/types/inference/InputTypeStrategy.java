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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;

/**
 * Strategy for inferring missing or incomplete input argument data types in a function call.
 *
 * <p>This interface has two responsibilities:
 *
 * <p>In the {@link TypeInference} process, it is called before the validation of input arguments and
 * can help in resolving the type of untyped {@code NULL} literals.
 *
 * <p>During the planning process, it can help in resolving the actual {@link DataType} including the
 * conversion class that a function implementation expects from the runtime. This requires that a
 * strategy can also be called on already validated arguments without affecting the logical type. This
 * is different from Calcite where unknown types are resolved first and might be overwritten by more
 * concrete types if available.
 *
 * <p>Note: Implementations should implement {@link Object#hashCode()} and {@link Object#equals(Object)}.
 *
 * @see InputTypeStrategies
 */
@PublicEvolving
public interface InputTypeStrategy {

	/**
	 * Infers the argument types of a function call.
	 */
	void inferInputTypes(MutableCallContext callContext);
}
