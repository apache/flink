package eu.stratosphere.sopremo.pact;

import java.util.Iterator;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.util.ConversionIterator;

public class WrapperIterator extends ConversionIterator<PactRecord, JsonNode> {

	public WrapperIterator(Iterator<PactRecord> iterator) {
		super(iterator);
	}

	@Override
	protected JsonNode convert(PactRecord record) {
		return ((JsonNodeWrapper) inputObject).getValue();
	}

}
