package eu.stratosphere.sopremo.pact;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.base.PactNull;

public class KeyRemover extends SopremoMap<PactJsonObject.Key, PactJsonObject, PactNull, PactJsonObject> {

	@Override
	public void map(PactJsonObject.Key key, PactJsonObject value, Collector<PactNull, PactJsonObject> out) {
		out.collect(PactNull.getInstance(), value);
	}

}
