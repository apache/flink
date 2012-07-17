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
package eu.stratosphere.meteor;

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

	private final JsonStream stream;

	private final EvaluationExpression indexExpression;

	/**
	 * Initializes StreamIndexExpression.
	 */
	public StreamIndexExpression(final JsonStream stream, final EvaluationExpression indexExpression) {
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
	
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result +  indexExpression.hashCode();
		result = prime * result +  stream.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		StreamIndexExpression other = (StreamIndexExpression) obj;
		return indexExpression.equals(other.indexExpression) ; // && stream.equals(other.stream)
	}

	
	@Override
	public void toString(StringBuilder builder) {
		this.appendTags(builder);
		builder.append(stream).append("[");
		indexExpression.toString(builder);
		builder.append("]");
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
