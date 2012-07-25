package eu.stratosphere.sopremo.packages;

public class DefaultNameChooser implements NameChooser {
	private final int[] preferredOrder;

	public DefaultNameChooser() {
		this(3, 0, 1, 2);
	}

	public DefaultNameChooser(final int... preferredOrder) {
		this.preferredOrder = preferredOrder;
	}

	@Override
	public String choose(final String[] nouns, final String[] verbs, final String[] adjectives,
			final String[] prepositions) {
		final String[][] names = { nouns, verbs, adjectives, prepositions };
		for (final int pos : this.preferredOrder) {
			final String value = this.firstOrNull(names[pos]);
			if (value != null)
				return value;
		}
		return null;
	}

	private String firstOrNull(final String[] names) {
		return names == null || names.length == 0 ? null : names[0];
	}
}