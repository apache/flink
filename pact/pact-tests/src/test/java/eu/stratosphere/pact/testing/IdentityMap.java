package eu.stratosphere.pact.testing;

import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

/**
 * Trivial PACT stub which emits the pairs without modifications.
 * 
 * @author Arvid Heise
 */
@SameKey
public class IdentityMap extends MapStub<Key, Value, Key, Value> {
	@Override
	public void map(Key key, Value value, Collector<Key, Value> out) {
		out.collect(key, value);
	}
}
