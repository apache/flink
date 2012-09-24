/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.base;

import java.util.Arrays;

import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.JsonStream;

/**
 * Some convenience method to implement complex operators.
 * 
 * @author Arvid Heise
 */
public class OperatorUtil {
	public static JsonStream positionEncode(JsonStream input, int index, int maxIndex) {
		final EvaluationExpression[] elements = new EvaluationExpression[maxIndex];
		Arrays.fill(elements, ConstantExpression.MISSING);
		elements[index] = EvaluationExpression.VALUE;
		return new Projection().
			withResultProjection(new ArrayCreation(elements)).
			withInputs(input);
	}

}
