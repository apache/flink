package eu.stratosphere.sopremo.operator;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactNull;

public class KeyExtractionStub extends SopremoMap<PactNull, PactJsonObject, Key, PactJsonObject> {
	@Override
	public void map(PactNull key, PactJsonObject value, Collector<Key, PactJsonObject> out) {
		out.collect(PactJsonObject.keyOf(this.getTransformation().evaluate(value.getValue(), this.getContext())), value);
	}
}