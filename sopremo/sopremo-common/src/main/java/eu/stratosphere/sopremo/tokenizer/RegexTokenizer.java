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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.stratosphere.sopremo.type.CachingArrayNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author Arvid Heise
 */
public class RegexTokenizer extends AbstractTokenizer {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8089912054552780511L;

	public final static Pattern WHITESPACE_PATTERN = Pattern.compile("\\p{javaWhitespace}+");

	private Pattern pattern = WHITESPACE_PATTERN;

	/**
	 * Initializes RegexTokenizer.
	 */
	public RegexTokenizer() {
	}

	/**
	 * Initializes RegexTokenizer.
	 * 
	 * @param pattern
	 */
	public RegexTokenizer(Pattern pattern) {
		this.pattern = pattern;
	}

	/**
	 * Returns the pattern.
	 * 
	 * @return the pattern
	 */
	public Pattern getPattern() {
		return this.pattern;
	}

	/**
	 * Sets the pattern to the specified value.
	 * 
	 * @param pattern
	 *        the pattern to set
	 */
	public void setPattern(Pattern pattern) {
		if (pattern == null)
			throw new NullPointerException("pattern must not be null");

		this.pattern = pattern;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.tokenizer.Tokenizer#tokenizeInto(java.lang.CharSequence,
	 * eu.stratosphere.sopremo.type.CachingArrayNode)
	 */
	@Override
	public void tokenizeInto(CharSequence text, CachingArrayNode tokens) {
		final Matcher matcher = this.pattern.matcher(text);

		tokens.clear();
		if (!matcher.find()) {
			addToken(tokens, text, 0, text.length());
			return;
		}

		int start = 0, end = 0;
		do {
			end = matcher.start();
			if (end > start) {
				addToken(tokens, text, start, end);
			}
			start = matcher.end();
		} while (matcher.find());
		addToken(tokens, text, start, text.length());
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		builder.append("RegexTokenizer [pattern=").append(this.pattern).append("]");
	}

}
