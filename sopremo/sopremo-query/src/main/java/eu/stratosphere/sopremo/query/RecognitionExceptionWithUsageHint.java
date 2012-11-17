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
package eu.stratosphere.sopremo.query;

import org.antlr.runtime.IntStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;

/**
 * @author Arvid Heise
 */
public class RecognitionExceptionWithUsageHint extends RecognitionException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6238176868329971376L;

	private String message;

	public RecognitionExceptionWithUsageHint(IntStream input, String message) {
		super(input);
		this.message = message;
	}
	
	public RecognitionExceptionWithUsageHint(Token token, String message) {
		super(token.getInputStream());
		this.token = token;
		this.line = token.getLine();
		this.charPositionInLine = token.getCharPositionInLine();
		this.message = message;
	}

	@Override
	public String getMessage() {
		StringBuilder builder = new StringBuilder(this.message);
		builder.append(" @ line: ").append(this.line).append("; row: ").append(this.charPositionInLine);
		if (this.token != null)
			builder.append("; but found ").append(this.token.getText());
		return builder.toString();
	}
}
