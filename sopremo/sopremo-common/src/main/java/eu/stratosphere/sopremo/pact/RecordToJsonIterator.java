package eu.stratosphere.sopremo.pact;

import java.util.Iterator;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.IJsonNode;

public class RecordToJsonIterator implements Iterator<IJsonNode> {

	private final Schema schema;

	private IJsonNode lastNode;

	private Iterator<PactRecord> iterator;

	public RecordToJsonIterator(final Schema schema) {
		this.schema = schema;
	}

	/**
	 * Sets the iterator to the specified value.
	 * 
	 * @param iterator
	 *        the iterator to set
	 */
	public void setIterator(final Iterator<PactRecord> iterator) {
		this.iterator = iterator;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return this.iterator.hasNext();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public IJsonNode next() {
		return this.schema.recordToJson(this.iterator.next(), this.lastNode);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		this.iterator.remove();
	}

}
