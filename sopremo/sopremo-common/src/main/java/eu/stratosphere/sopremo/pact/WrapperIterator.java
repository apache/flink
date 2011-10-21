package eu.stratosphere.sopremo.pact;

import java.util.Iterator;

import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.util.ConversionIterator;

public class WrapperIterator extends ConversionIterator<JsonNode, JsonNode> {

	public WrapperIterator(Iterator<JsonNode> iterator) {
		super(iterator);
	}

	@Override
	protected JsonNode convert(JsonNode inputObject) {
		return ((JsonNodeWrapper) inputObject).getValue();
	}

}
