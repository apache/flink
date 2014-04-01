package eu.stratosphere.pact.runtime.test.util.types;

import eu.stratosphere.api.common.typeutils.TypePairComparator;

public class StringPairPairComparator extends
		TypePairComparator<StringPair, StringPair> {
	
	private String reference;

	@Override
	public void setReference(StringPair reference) {
		this.reference = reference.getKey();
	}

	@Override
	public boolean equalToReference(StringPair candidate) {
		return reference.equals(candidate.getKey());
	}

	@Override
	public int compareToReference(StringPair candidate) {
		return reference.compareTo(candidate.getKey());
	}

}
