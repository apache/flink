package eu.stratosphere.pact.runtime.hash;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypePairComparator;

/**
 * @param <PT> probe side type
 * @param <BT> build side type
 */
public abstract class AbstractHashTableProber<PT, BT> {
	
	protected final TypeComparator<PT> probeTypeComparator;
	
	protected final TypePairComparator<PT, BT> pairComparator;
	
	public AbstractHashTableProber(TypeComparator<PT> probeTypeComparator, TypePairComparator<PT, BT> pairComparator) {
		this.probeTypeComparator = probeTypeComparator;
		this.pairComparator = pairComparator;
	}
	
	public abstract boolean getMatchFor(PT probeSideRecord, BT targetForMatch);
	
	public abstract void updateMatch(BT record) throws IOException;
}
