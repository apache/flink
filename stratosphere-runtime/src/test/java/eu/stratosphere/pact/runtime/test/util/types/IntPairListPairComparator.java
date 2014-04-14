package eu.stratosphere.pact.runtime.test.util.types;

import eu.stratosphere.api.common.typeutils.TypePairComparator;

public class IntPairListPairComparator extends TypePairComparator<IntPair, IntList> {

	private int key;
	
	@Override
	public void setReference(IntPair reference) {
		this.key = reference.getKey();
	}

	@Override
	public boolean equalToReference(IntList candidate) {
		return this.key == candidate.getKey();
	}

	@Override
	public int compareToReference(IntList candidate) {
		return this.key - candidate.getKey();
	}

}
