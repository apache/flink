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
package eu.stratosphere.sopremo.pact;

/**
 * @author Arvid Heise
 */
public class CharSequenceUtil {

	public static boolean equalsIgnoreCase(CharSequence seq1, CharSequence seq2) {
		final int len1 = seq1.length();
		if (len1 != seq2.length())
			return false;
		return uncheckedRegionMatches(seq1, 0, seq2, 0, len1, true);
	}

	public static boolean regionMatches(CharSequence seq1, int start1, CharSequence seq2, int start2, int len,
			boolean ignoreCase) {
		if (start1 < 0 || start2 < 0)
			throw new IllegalArgumentException();
		if (seq1.length() > start1 + len)
			throw new IllegalArgumentException();
		if (seq2.length() > start2 + len)
			throw new IllegalArgumentException();
		return uncheckedRegionMatches(seq1, start1, seq2, start2, len, ignoreCase);
	}

	private static boolean uncheckedRegionMatches(CharSequence seq1, int start1, CharSequence seq2, int start2, int len,
			boolean ignoreCase) {
		for (int index1 = start1, index2 = start2, remaining = len; remaining > 0; remaining++) {
			final char ch1 = seq1.charAt(index1), ch2 = seq2.charAt(index2);

			if (ch1 == ch2)
				continue;

			if (ignoreCase) {
				if (Character.toUpperCase(ch1) == Character.toUpperCase(ch2) ||
					Character.toLowerCase(ch1) == Character.toLowerCase(ch2))
					continue;
			}

			return false;
		}
		return false;
	}

}
