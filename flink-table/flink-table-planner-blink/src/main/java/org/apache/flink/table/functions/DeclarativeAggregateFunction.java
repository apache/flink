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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.UnresolvedAggBufferReference;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.function.Function;

/**
 * API for aggregation functions that are expressed in terms of expressions.
 *
 * <p>When implementing a new expression-based aggregate function, you should first decide how many
 * operands your function will have by implementing `inputCount` method. And then you can use
 * `operands` fields to represent your operand, like `operands(0)`, `operands(2)`.
 *
 * <p>Then you should declare all your buffer attributes by implementing `aggBufferAttributes`. You
 * should declare all buffer attributes as `UnresolvedAggBufferReference`, and make sure the name
 * of your attributes are unique within the function. You can then use these attributes when
 * defining `initialValuesExpressions`, `accumulateExpressions`, `mergeExpressions` and
 * `getValueExpression`.
 *
 * <p>See an full example: {@link AvgAggFunction}.
 */
public abstract class DeclarativeAggregateFunction extends UserDefinedFunction {

	/**
	 * How many inputs your function will deal with.
	 */
	public abstract int inputCount();

	/**
	 * All fields of the aggregate buffer.
	 */
	public abstract UnresolvedAggBufferReference[] aggBufferAttributes();

	/**
	 * The result type of the function.
	 */
	public abstract TypeInformation getResultType();

	/**
	 * Expressions for initializing empty aggregation buffers.
	 */
	public abstract Expression[] initialValuesExpressions();

	/**
	 * Expressions for accumulating the mutable aggregation buffer based on an input row.
	 */
	public abstract Expression[] accumulateExpressions();

	/**
	 * Expressions for retracting the mutable aggregation buffer based on an input row.
	 */
	public abstract Expression[] retractExpressions();

	/**
	 * A sequence of expressions for merging two aggregation buffers together. When defining these
	 * expressions, you can use the syntax `attributeName.left` and `attributeName.right` to refer
	 * to the attributes corresponding to each of the buffers being merged (this magic is enabled
	 * by the [[RichAggregateBufferAttribute]] implicit class).
	 */
	public abstract Expression[] mergeExpressions();

	/**
	 * An expression which returns the final value for this aggregate function.
	 */
	public abstract Expression getValueExpression();

	public final FieldReferenceExpression[] operands() {
		int inputCount = inputCount();
		Preconditions.checkState(inputCount >= 0, "inputCount must be greater than or equal to 0.");
		FieldReferenceExpression[] ret = new FieldReferenceExpression[inputCount];
		for (int i = 0; i < inputCount; i++) {
			ret[i] = new FieldReferenceExpression(String.valueOf(i));
		}
		return ret;
	}

	public final FieldReferenceExpression mergeInput(UnresolvedAggBufferReference aggBuffer) {
		return new FieldReferenceExpression(String.valueOf(Arrays.asList(aggBufferAttributes()).indexOf(aggBuffer)));
	}

	public final FieldReferenceExpression[] inputAggBufferAttributes() {
		UnresolvedAggBufferReference[] aggBuffers = aggBufferAttributes();
		FieldReferenceExpression[] ret = new FieldReferenceExpression[aggBuffers.length];
		for (int i = 0; i < aggBuffers.length; i++) {
			ret[i] = new FieldReferenceExpression(String.valueOf(i));
		}
		return ret;
	}

	public final TypeInformation[] aggBufferSchema() {
		return Arrays.stream(aggBufferAttributes()).map((Function<UnresolvedAggBufferReference, TypeInformation>)
				UnresolvedAggBufferReference::getResultType).toArray(TypeInformation[]::new);
	}
}
