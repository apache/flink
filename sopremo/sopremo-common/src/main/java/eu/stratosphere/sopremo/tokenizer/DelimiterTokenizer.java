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
package eu.stratosphere.sopremo.tokenizer;

import it.unimi.dsi.fastutil.chars.CharOpenHashSet;
import it.unimi.dsi.fastutil.chars.CharSet;

import java.util.Arrays;
import java.util.Collection;

import eu.stratosphere.sopremo.type.CachingArrayNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author Arvid Heise
 */
public class DelimiterTokenizer extends AbstractTokenizer implements Tokenizer {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1084185017823016725L;

	private final CharSet delimiters = new CharOpenHashSet();

	public static final DelimiterTokenizer WHITESPACES = new DelimiterTokenizer(' ', '\n', '\r', '\t');

	/**
	 * Initializes DelimiterTokenizer.
	 */
	public DelimiterTokenizer() {
	}

	/**
	 * Initializes DelimiterTokenizer.
	 * 
	 * @param delimiters
	 */
	public DelimiterTokenizer(Collection<Character> delimiters) {
		this.delimiters.addAll(delimiters);
	}

	/**
	 * Initializes DelimiterTokenizer.
	 * 
	 * @param delimiters
	 */
	public DelimiterTokenizer(Character... delimiters) {
		this(Arrays.asList(delimiters));
	}

	public DelimiterTokenizer addDelimiter(char delimiter) {
		this.delimiters.add(delimiter);
		return this;
	}

	/**
	 * Sets the delimiters to the specified value.
	 * 
	 * @param delimiters
	 *        the delimiters to set
	 */
	public void setDelimiters(Collection<Character> delimiters) {
		if (delimiters == null)
			throw new NullPointerException("delimiters must not be null");

		this.delimiters.clear();
		this.delimiters.addAll(delimiters);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append("DelimiterTokenizer [");
		builder.append("delimiters=").append(this.delimiters);
		builder.append("]");
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.tokenizer.Tokenizer#tokenizeInto(java.lang.CharSequence, eu.stratosphere.sopremo.type.CachingArrayNode)
	 */
	@Override
	public void tokenizeInto(CharSequence text, CachingArrayNode tokens) {
		tokens.setSize(0);

		int textIndex = 0, tokenStart = 0;
		for (; textIndex < text.length(); textIndex++) {
			final char ch = text.charAt(textIndex);
			if (this.delimiters.contains(ch)) {
				if (textIndex == tokenStart)
					tokenStart++;
				else {
					addToken(tokens, text, tokenStart, textIndex);
				}
			}
		}

		if (textIndex != tokenStart)
			addToken(tokens, text, tokenStart, textIndex);
	}
}
