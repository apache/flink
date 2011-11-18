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
package eu.stratosphere.simple.jaql;

import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.UnevaluableExpression;

/**
 * @author Arvid Heise
 */
public class StreamIndexExpression extends UnevaluableExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1317711966846556055L;

	private JsonStream stream;

	private EvaluationExpression indexExpression;

	/**
	 * Initializes StreamIndexExpression.
	 */
	public StreamIndexExpression(JsonStream stream, EvaluationExpression indexExpression) {
		super("Stream index");
		this.stream = stream;
		this.indexExpression = indexExpression;
	}

	/**
	 * Returns the indexExpression.
	 * 
	 * @return the indexExpression
	 */
	public EvaluationExpression getIndexExpression() {
		return this.indexExpression;
	}

	/**
	 * Returns the stream.
	 * 
	 * @return the stream
	 */
	public JsonStream getStream() {
		return this.stream;
	}
}
