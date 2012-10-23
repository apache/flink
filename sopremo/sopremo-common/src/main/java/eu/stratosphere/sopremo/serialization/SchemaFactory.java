/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.sopremo.serialization;

import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

/**
 * @author Arvid Heise
 */
public interface SchemaFactory extends ISerializableSopremoType {

	/**
	 * This method takes keyExpressions in form of EvaluationExpressions and tries to give back a matching
	 * {@link Schema}
	 * 
	 * @param keyExpressions
	 *        the Expressions, from which a Schema shall be created
	 * @return {@link Schema}, corresponding to the <code>keyExpressions</code>
	 */
	Schema create(Iterable<EvaluationExpression> keyExpressions);

}
