package eu.stratosphere.nephele.services.accumulators;

import java.util.HashMap;
import java.util.Map;

public class AccumulatorHelper {

	/**
	 * Merge two collections of accumulators. The second will be merged into the
	 * first.
	 * 
	 * @param target
	 *            The collection of accumulators that will be updated
	 * @param toMerge
	 *            The collection of accumulators that will be merged into the
	 *            other
	 */
	public static void mergeInto(Map<String, Accumulator<?, ?>> target,
			Map<String, Accumulator<?, ?>> toMerge) {
		for (Map.Entry<String, Accumulator<?, ?>> otherEntry : toMerge.entrySet()) {
			Accumulator<?, ?> ownAccumulator = target.get(otherEntry.getKey());
			if (ownAccumulator == null) {
				// Take over counter from chained task
				target.put(otherEntry.getKey(), otherEntry.getValue());
			} else {
				// Both should have the same type
				AccumulatorHelper.compareAccumulatorTypes(otherEntry.getKey(),
						ownAccumulator.getClass(), otherEntry.getValue().getClass());
				// Merge counter from chained task into counter from stub
				mergeSingle(ownAccumulator, otherEntry.getValue());
			}
		}
	}

	/**
	 * Workaround method for type safety
	 */
	private static final <V, R> void mergeSingle(Accumulator<?, ?> target, Accumulator<?, ?> toMerge) {
		@SuppressWarnings("unchecked")
		Accumulator<V, R> typedTarget = (Accumulator<V, R>) target;

		@SuppressWarnings("unchecked")
		Accumulator<V, R> typedToMerge = (Accumulator<V, R>) toMerge;

		typedTarget.merge(typedToMerge);
	}

	/**
	 * Compare both classes and throw {@link UnsupportedOperationException} if
	 * they differ
	 */
	public static void compareAccumulatorTypes(Object name,
			@SuppressWarnings("rawtypes") Class<? extends Accumulator> first,
			@SuppressWarnings("rawtypes") Class<? extends Accumulator> second)
			throws UnsupportedOperationException {
		if (first != second) {
			throw new UnsupportedOperationException("The accumulator object '" + name
					+ "' was created with two different types: " + first + " and " + second);
		}
	}

	/**
	 * Transform the Map with accumulators into a Map containing only the
	 * results
	 */
	public static Map<String, Object> toResultMap(Map<String, Accumulator<?, ?>> accumulators) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		for (Map.Entry<String, Accumulator<?, ?>> entry : accumulators.entrySet()) {
			resultMap.put(entry.getKey(), (Object) entry.getValue().getLocalValue());
		}
		return resultMap;
	}

	public static String getAccumulatorsFormated(Map<?, Accumulator<?, ?>> newAccumulators) {
		StringBuilder builder = new StringBuilder();
		for (Map.Entry<?, Accumulator<?, ?>> entry : newAccumulators.entrySet()) {
			builder.append("- " + entry.getKey() + " (" + entry.getValue().getClass().getName()
					+ ")" + ": " + entry.getValue().toString() + "\n");
		}
		return builder.toString();
	}

	public static String getResultsFormated(Map<String, Object> map) {
		StringBuilder builder = new StringBuilder();
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			builder.append("- " + entry.getKey() + " (" + entry.getValue().getClass().getName()
					+ ")" + ": " + entry.getValue().toString() + "\n");
		}
		return builder.toString();
	}

	public static void resetAndClearAccumulators(
			Map<String, Accumulator<?, ?>> accumulators) {
		for (Map.Entry<String, Accumulator<?, ?>> entry : accumulators.entrySet()) {
			entry.getValue().resetLocal();
		}
		accumulators.clear();
	}

}
