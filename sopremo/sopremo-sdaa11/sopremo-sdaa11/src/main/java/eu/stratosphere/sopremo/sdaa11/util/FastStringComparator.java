package eu.stratosphere.sopremo.sdaa11.util;

import java.util.Comparator;

/**
 * At first tries to sort strings by their length and by that means speed up
 * comparisons.
 */
public class FastStringComparator implements Comparator<String> {

	public static final FastStringComparator INSTANCE = new FastStringComparator();

	private FastStringComparator() {
	}

	@Override
	public int compare(final String str1, final String str2) {
		final int diff = str1.length() - str2.length();
		if (diff != 0)
			return diff;
		return str1.compareTo(str2);
	}

}
