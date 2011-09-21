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

package eu.stratosphere.pact.example.wordcount;

import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.MutableObjectIterator;


/**
 * 
 */
public class AsciiUtils
{
	/**
	 * Converts the given <code>PactString</code> into a lower case variant.
	 * <p>
	 * NOTE: This method assumes that the string contains only characters that are valid in the
	 * ASCII type set.
	 * 
	 * @param string The string to convert to lower case.
	 */
	public static void toLowerCase(PactString string)
	{
		final char[] chars = string.getChars();
		final int len = string.length();
		
		for (int i = 0; i < len; i++) {
			chars[i] = Character.toLowerCase(chars[i]);
		}
	}
	
	public static void replaceNonWordChars(PactString string, char replacement)
	{
		final char[] chars = string.getChars();
		final int len = string.length();
		
		for (int i = 0; i < len; i++) {
			final char c = chars[i];
			if (!(Character.isLetter(c) || Character.isDigit(c) || c == '_')) {
				chars[i] = replacement;
			}
		}
	}
	
	// ============================================================================================
	
	public static final class WhitespaceTokenizer implements MutableObjectIterator<PactString>
	{		
		private PactString toTokenize;
		private int pos;
		private int limit;
		
		public WhitespaceTokenizer()
		{}
		
		public void setStringToTokenize(PactString string)
		{
			this.toTokenize = string;
			this.pos = 0;
			this.limit = string.length();
		}
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.util.MutableObjectIterator#next(java.lang.Object)
		 */
		@Override
		public boolean next(PactString target)
		{
			final char[] data = this.toTokenize.getChars();
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
	
	private AsciiUtils() {}
}
