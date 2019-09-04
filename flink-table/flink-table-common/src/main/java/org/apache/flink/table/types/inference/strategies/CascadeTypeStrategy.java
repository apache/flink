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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.inference.TypeTransformation;

import java.util.List;
import java.util.Optional;

/**
 * Strategy to infer the data type of a function call from the type of the operands
 * by using one {@link TypeStrategy} rule and a combination of
 * {@link TypeTransformation}s applied onto its result.
 */
@Internal
public class CascadeTypeStrategy implements TypeStrategy {

	private final TypeStrategy typeStrategy;
	private final List<TypeTransformation> typeTransformations;

	public CascadeTypeStrategy(
			TypeStrategy typeStrategy,
			List<TypeTransformation> typeTransformations) {
		this.typeStrategy = typeStrategy;
		this.typeTransformations = typeTransformations;
	}

	@Override
	public Optional<DataType> inferType(CallContext callContext) {
		return typeStrategy.inferType(callContext).map(
			type -> {
				DataType transformedType = type;
				for (TypeTransformation transformation : typeTransformations) {
					transformedType = transformation.transform(callContext, transformedType);
				}

				return transformedType;
			}
		);
	}
}
