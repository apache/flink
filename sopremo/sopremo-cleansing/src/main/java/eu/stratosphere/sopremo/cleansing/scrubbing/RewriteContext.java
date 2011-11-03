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

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.PathExpression;

/**
 * @author Arvid Heise
 */
public class RewriteContext extends EvaluationContext {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3751142957269653244L;

	private PathExpression rewritePath;

	public PathExpression getRewritePath() {
		return this.rewritePath;
	}

	public void setRewritePath(PathExpression rewritePath) {
		if (rewritePath == null)
			throw new NullPointerException("rewriteContext must not be null");

		this.rewritePath = rewritePath;
	}

}
