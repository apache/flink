package eu.stratosphere.sopremo.serialization;

import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Arrays;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;

/**
 * Base class for all schema that build upon {@link JsonNodeWrapper}.
 * 
 * @author Arvid Heise
 */
public abstract class AbstractSchema implements Schema {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1012715697040531298L;

	private final Class<? extends Value>[] pactSchema;

	private IntSet keyIndices;

	@SuppressWarnings("unchecked")
	protected AbstractSchema(final int numFields, final IntSet keyIndices) {
		if (keyIndices == null)
			throw new NullPointerException();
		this.keyIndices = keyIndices;
		this.pactSchema = new Class[numFields];
		Arrays.fill(this.pactSchema, JsonNodeWrapper.class);
	}

	@Override
	public IntSet getKeyIndices() {
		return this.keyIndices;
	}

	@Override
	public Class<? extends Value>[] getPactSchema() {
		return this.pactSchema;
	}
}
