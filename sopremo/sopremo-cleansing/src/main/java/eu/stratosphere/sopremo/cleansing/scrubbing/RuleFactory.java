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
package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.PathExpression;

/**
 * @author Arvid Heise
 */
public interface RuleFactory {
	public static class RuleContext {
		private Operator<?> operator;
		private PathExpression contextPath;
		private RuleManager manager;		
		private RuleFactory ruleFactory;
		
		public RuleContext(Operator<?> operator, PathExpression contextPath, RuleManager manager, RuleFactory ruleFactory) {
			this.operator = operator;
			this.contextPath = contextPath;
			this.manager = manager;
			this.ruleFactory = ruleFactory;
		}

		public Operator<?> getOperator() {
			return operator;
		}
		
		public RuleFactory getRuleFactory() {
			return ruleFactory;
		}
		
		public PathExpression getContextPath() {
			return contextPath;
		}
		
		public RuleManager getManager() {
			return manager;
		}
		
		public RuleContext withPath(PathExpression path) {
			return new RuleContext(operator, path, manager, ruleFactory);
		}
	}
	/**
	 * @param expression
	 * @param manager TODO
	 * @param context
	 * @return
	 */
	public EvaluationExpression createRule(EvaluationExpression expression, RuleContext context);
}
