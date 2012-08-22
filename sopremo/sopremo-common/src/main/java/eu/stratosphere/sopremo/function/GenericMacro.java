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
package eu.stratosphere.sopremo.function;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * @author Arvid Heise
 */
public class GenericMacro extends MacroBase {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7262633375508859703L;

	private final Class<? extends EvaluationExpression> expressionClass;

	/**
	 * Initializes GenericMacro.
	 * 
	 * @param name
	 * @param macroParams
	 */
	public GenericMacro(final Class<? extends EvaluationExpression> expressionClass) {
		this.expressionClass = expressionClass;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.function.Callable#call(InputType[], eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public EvaluationExpression call(final EvaluationExpression[] params, final EvaluationExpression target,
			final EvaluationContext context) {
		return ReflectUtil.newInstance(this.expressionClass, (Object[]) params);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append("Macro generating ").append(this.expressionClass.getSimpleName());
	}
}
