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
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
public class ReplacingMacro extends MacroBase {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3949887310463840434L;

	private final EvaluationExpression replacement;

	public ReplacingMacro(final String name, final EvaluationExpression replacement) {
		super(name);
		this.replacement = replacement;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public EvaluationExpression call(final EvaluationExpression[] params, final EvaluationExpression target,
			final EvaluationContext context) {
		return this.replacement;
	}
}
