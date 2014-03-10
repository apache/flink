/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.util;

import java.io.Serializable;

import eu.stratosphere.types.StringValue;


/**
 * Utility class for efficient string operations on strings. All methods in this class are
 * written to be optimized for efficiency and work only on strings whose characters are
 * representable in a single <tt>char</tt>, ie. strings without surrogate characters.
 */
public final class SimpleStringUtils {
	
	/**
	 * Converts the given <code>StringValue</code> into a lower case variant.
	 * 
	 * @param string The string to convert to lower case.
	 */
	public static void toLowerCase(StringValue string) {
		final char[] chars = string.getCharArray();
		final int len = string.length();
		
		for (int i = 0; i < len; i++) {
			chars[i] = Character.toLowerCase(chars[i]);
		}
	}
	
	/**
	 * Replaces all non-word characters in a string by a given character. The only
	 * characters not replaced are the characters that qualify as word characters
	 * or digit characters with respect to {@link Character#isLetter(char)} or
	 * {@link Character#isDigit(char)}, as well as the underscore character.
	 * <p>
	 * This operation is intended to simplify strings for counting distinct words.
	 * 
	 * @param string The string value to have the non-word characters replaced.
	 * @param replacement The character to use as the replacement.
	 */
	public static void replaceNonWordChars(StringValue string, char replacement) {
		final char[] chars = string.getCharArray();
		final int len = string.length();
		
		for (int i = 0; i < len; i++) {
			final char c = chars[i];
			if (!(Character.isLetter(c) || Character.isDigit(c) || c == '_')) {
				chars[i] = replacement;
			}
		}
	}
	
	// ============================================================================================
	
	/**
	 * A tokenizer for string values that uses whitespace characters as token delimiters.
	 * The tokenizer is designed to have a resettable state and operate on mutable objects,
	 * sparing object allocation and garbage collection overhead.
	 */
	public static final class WhitespaceTokenizer implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private StringValue toTokenize;		// the string to tokenize
		private int pos;					// the current position in the string
		private int limit;					// the limit in the string's character data
		
		/**
		 * Creates a new tokenizer with an undefined internal state.
		 */
		public WhitespaceTokenizer() {}
		
		/**
		 * Sets the string to be tokenized and resets the state of the tokenizer.
		 * 
		 * @param string The string value to be tokenized.
		 */
		public void setStringToTokenize(StringValue string) {
			this.toTokenize = string;
			this.pos = 0;
			this.limit = string.length();
		}
		
		/**
		 * Gets the next token from the string. If another token is available, the token is stored
		 * in the given target StringValue object.
		 * 
		 * @param target The StringValue object to store the next token in.
		 * @return True, if there was another token, false if not.
		 */
		public boolean next(StringValue target) {
			final char[] data = this.toTokenize.getCharArray();
			final int limit = this.limit;
			int pos = this.pos;
			
			// skip the delimiter
			for (; pos < limit && Character.isWhitespace(data[pos]); pos++);
			
			if (pos >= limit) {
				this.pos = pos;
				return false;
			}
			
			final int start = pos;
			for (; pos < limit && !Character.isWhitespace(data[pos]); pos++);
			this.pos = pos;
			target.setValue(this.toTokenize, start, pos - start);
			return true;
		}
	}
	
	// ============================================================================================
	
	/**
	 * Private constructor to prevent instantiation, as this is a utility method encapsulating class.
	 */
	private SimpleStringUtils() {}
}
