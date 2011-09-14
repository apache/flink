package eu.stratosphere.sopremo;

import java.util.Iterator;

import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.util.ConversionIterator;

/**
 * Provides a facade around an {@link Iterator} of {@link PactJsonObject} that unwraps the {@link JsonNode}
 * element-wise.
 * 
 * @author Arvid Heise
 */
public class UnwrappingIterator extends ConversionIterator<PactJsonObject, JsonNode> {
	/**
	 * Initializes UnwrappingIterator as a facade of the provided {@link Iterator}.
	 * 
	 * @param iterator
	 *        the iterator to wrap
	 */
	public UnwrappingIterator(final Iterator<PactJsonObject> iterator) {
		super(iterator);
	}

	@Override
	protected JsonNode convert(final PactJsonObject inputObject) {
		return inputObject.getValue();
	}
}