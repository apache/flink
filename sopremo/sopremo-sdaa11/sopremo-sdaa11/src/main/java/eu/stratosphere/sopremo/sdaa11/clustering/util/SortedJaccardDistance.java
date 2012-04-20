package eu.stratosphere.sopremo.sdaa11.clustering.util;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class SortedJaccardDistance {

	public static final int MAX_DISTANCE = 1000;

	/**
	 * Calculates the Jaccard distance of two sorted lists (according to the
	 * comparator).
	 */
	public static <T> int distance(final List<T> list1, final List<T> list2,
			final Comparator<T> comparator) {
		if (list1.size() + list2.size() == 0)
			return 0;
		final int intersectionSize = intersectionSize(list1, list2, comparator);
		return MAX_DISTANCE
				* (list1.size() + list2.size() - 2 * intersectionSize)
				/ (list1.size() + list2.size() - intersectionSize);
	}

	/**
	 * Counts the common elements of two lists that are sorted according to the
	 * comparator.
	 */
	public static <T> int intersectionSize(final List<T> list1,
			final List<T> list2, final Comparator<T> comparator) {

		if (list1.isEmpty() || list2.isEmpty())
			return 0;

		int diff;
		int intersectionSize = 0;

		final Iterator<T> i1 = list1.iterator(), i2 = list2.iterator();
		T e1 = i1.next(), e2 = i2.next();
		boolean keepRunning = true;

		while (keepRunning) {
			diff = comparator.compare(e1, e2);
			if (diff == 0)
				intersectionSize++;

			// e1 <= e2 --> move i1 forward
			if (diff <= 0)
				if (i1.hasNext())
					e1 = i1.next();
				else
					keepRunning = false;

			// e1 >= e2 --> move i2 forward
			if (diff >= 0)
				if (i2.hasNext())
					e2 = i2.next();
				else
					keepRunning = false;
		}

		return intersectionSize;
	}
}
