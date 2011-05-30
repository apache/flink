package eu.stratosphere.sopremo;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.pact.SopremoMap;

public class KeyRemover extends SopremoMap<PactJsonObject.Key, PactJsonObject, PactNull, PactJsonObject> {

	@Override
	public void map(Key key, PactJsonObject value, Collector<PactNull, PactJsonObject> out) {
		out.collect(PactNull.getInstance(), value);
	}

}
