package eu.stratosphere.sopremo.operator;

import java.util.Iterator;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.util.AbstractIterator;

public class UnwrappingIterator extends AbstractIterator<JsonNode> {
	private final Iterator<PactJsonObject> values;

	public UnwrappingIterator(Iterator<PactJsonObject> values) {
		this.values = values;
	}

	@Override
	protected JsonNode loadNext() {
		if (!values.hasNext())
			return noMoreElements();
		return values.next().getValue();
	}
}