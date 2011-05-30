package eu.stratosphere.sopremo;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoMap;

public class KeyExtractionStub extends SopremoMap<PactNull, PactJsonObject, Key, PactJsonObject> {
	@Override
	public void map(PactNull key, PactJsonObject value, Collector<Key, PactJsonObject> out) {
		out.collect(PactJsonObject.keyOf(this.getTransformation().evaluate(value.getValue(), this.getContext())), value);
	}
}