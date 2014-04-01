package eu.stratosphere.pact.runtime.test.util.types;

import eu.stratosphere.api.common.typeutils.TypePairComparator;

public class IntListPairComparator extends TypePairComparator<IntList, IntList> {
	
	private int key;

	@Override
	public void setReference(IntList reference) {
		this.key = reference.getKey();
	}

	@Override
	public boolean equalToReference(IntList candidate) {
		return this.key == candidate.getKey();
	}

	@Override
	public int compareToReference(IntList candidate) {
		return candidate.getKey() - this.key;
	}

}
