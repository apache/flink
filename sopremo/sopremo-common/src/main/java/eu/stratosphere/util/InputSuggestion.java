package eu.stratosphere.util;

import it.unimi.dsi.fastutil.objects.AbstractObject2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class InputSuggestion<T> {
	private Map<CharSequence, T> possibleValues = new HashMap<CharSequence, T>();

	private SimilarityMeasure similarityMeasure = new Levensthein();

	private int maxSuggestions = Integer.MAX_VALUE;

	private double minSimilarity = 0;

	public InputSuggestion(final Map<String, T> possibleValues) {
		this.possibleValues.putAll(possibleValues);
	}

	public int getMaxSuggestions() {
		return this.maxSuggestions;
	}

	public double getMinSimilarity() {
		return this.minSimilarity;
	}

	public Map<CharSequence, T> getPossibleValues() {
		return this.possibleValues;
	}

	public SimilarityMeasure getSimilarityMeasure() {
		return this.similarityMeasure;
	}

	public void setMaxSuggestions(final int maxSuggestions) {
		if (maxSuggestions < 1)
			throw new IllegalArgumentException("maxSuggestions must >= 1");

		this.maxSuggestions = maxSuggestions;
	}

	public void setMinSimilarity(final double minSimilarity) {
		if (minSimilarity < 0 || minSimilarity > 1)
			throw new IllegalArgumentException("minSimilarity must be in [0; 1]");

		this.minSimilarity = minSimilarity;
	}

	public void setPossibleValues(final Map<CharSequence, T> possibleValues) {
		if (possibleValues == null)
			throw new NullPointerException("possibleValues must not be null");

		this.possibleValues = possibleValues;
	}

	public void setSimilarityMeasure(final SimilarityMeasure similarityMeasure) {
		if (similarityMeasure == null)
			throw new NullPointerException("similarityMeasure must not be null");

		this.similarityMeasure = similarityMeasure;
	}

	public List<T> suggest(final CharSequence input) {
		final List<T> suggestions = new ArrayList<T>();
		for (final Object2DoubleMap.Entry<T> entry : this.suggestWithProbability(input))
			suggestions.add(entry.getKey());
		return suggestions;
	}

	public List<Object2DoubleMap.Entry<T>> suggestWithProbability(final CharSequence input) {
		final List<Object2DoubleMap.Entry<T>> list = new ArrayList<Object2DoubleMap.Entry<T>>();

		// calculate similarity values for each possible value
		for (final Map.Entry<CharSequence, T> possibility : this.possibleValues.entrySet()) {
			final double similarity = this.similarityMeasure.getSimilarity(input, possibility.getKey(),
				this.minSimilarity);
			if (similarity >= this.minSimilarity)
				list.add(new AbstractObject2DoubleMap.BasicEntry<T>(possibility.getValue(), similarity));
		}

		// sort largest to smallest
		Collections.sort(list, new Comparator<Object2DoubleMap.Entry<T>>() {
			@Override
			public int compare(final Object2DoubleMap.Entry<T> o1, final Object2DoubleMap.Entry<T> o2) {
				return Double.compare(o2.getDoubleValue(), o1.getDoubleValue());
			}
		});

		return list.size() > this.maxSuggestions ? list.subList(0, this.maxSuggestions) : list;
	}

	public InputSuggestion<T> withMaxSuggestions(final int maxSuggestions) {
		this.setMaxSuggestions(maxSuggestions);
		return this;
	}

	public InputSuggestion<T> withMinSimilarity(final double minSimilarity) {
		this.setMinSimilarity(minSimilarity);
		return this;
	}

	public InputSuggestion<T> withSimilarityMeasure(final SimilarityMeasure similarityMeasure) {
		this.setSimilarityMeasure(similarityMeasure);
		return this;
	}

	public static class Levensthein implements SimilarityMeasure {
		@Override
		public double getSimilarity(final CharSequence input, final CharSequence possibleValue,
				final double minSimilarity) {
			final int length = Math.max(input.length(), possibleValue.length());
			final int threshold = (int) Math.ceil(length * (1 - minSimilarity));
			final int distance = StringUtils.getLevenshteinDistance(input, possibleValue, threshold);
			if (distance == -1)
				return 0;
			return 1 - (double) distance / length;
		}
	}

	public static interface SimilarityMeasure {
		public double getSimilarity(CharSequence input, CharSequence possibleValue, double minSimilarity);
	}
}
