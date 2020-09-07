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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.Collections;
import java.util.Set;

/**
 * Definition of a function. Instances of this class provide all details necessary to validate a function
 * call and perform planning.
 *
 * <p>A pure function definition must not contain a runtime implementation. This can be provided by
 * the planner at later stages.
 *
 * @see UserDefinedFunction
 */
@PublicEvolving
public interface FunctionDefinition {

	/**
	 * Returns the kind of function this definition describes.
	 */
	FunctionKind getKind();

	/**
	 * Returns the logic for performing type inference of a call to this function definition.
	 *
	 * <p>The type inference process is responsible for inferring unknown types of input arguments,
	 * validating input arguments, and producing result types. The type inference process happens
	 * independent of a function body. The output of the type inference is used to search for a
	 * corresponding runtime implementation.
	 *
	 * <p>Instances of type inference can be created by using {@link TypeInference#newBuilder()}.
	 *
	 * <p>See {@link BuiltInFunctionDefinitions} for concrete usage examples.
	 */
	TypeInference getTypeInference(DataTypeFactory typeFactory);

	/**
	 * Returns the set of requirements this definition demands.
	 */
	default Set<FunctionRequirement> getRequirements() {
		return Collections.emptySet();
	}

	/**
	 * Returns information about the determinism of the function's results.
	 *
	 * <p>It returns <code>true</code> if and only if a call to this function is guaranteed to
	 * always return the same result given the same parameters. <code>true</code> is
	 * assumed by default. If the function is not purely functional like <code>random(), date(), now(), ...</code>
	 * this method must return <code>false</code>.
	 *
	 * <p>Furthermore, return <code>false</code> if the planner should always execute this function
	 * on the cluster side. In other words: the planner should not perform constant expression reduction
	 * during planning for constant calls to this function.
	 */
	default boolean isDeterministic() {
		return true;
	}
}
