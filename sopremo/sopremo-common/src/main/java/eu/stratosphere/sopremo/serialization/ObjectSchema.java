package eu.stratosphere.sopremo.serialization;

import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 * @author Arvid Heise
 */
public class ObjectSchema extends AbstractSchema {

	private static final long serialVersionUID = 4037447354469753483L;

	private final List<String> mappings = new ArrayList<String>();

	public ObjectSchema(final List<ObjectAccess> mappings) {
		super(mappings.size() + 1, rangeFrom(0, mappings.size()));
		for (final ObjectAccess mapping : mappings)
			this.mappings.add(mapping.getField());
	}

	public ObjectSchema(final String... mappings) {
		super(mappings.length + 1, rangeFrom(0, mappings.length));
		for (final String mapping : mappings)
			this.mappings.add(mapping);
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final ObjectSchema other = (ObjectSchema) obj;
		return this.mappings.equals(other.mappings);
	}

	public List<String> getMappings() {
		return this.mappings;
	}

	public int getMappingSize() {
		return this.mappings.size();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.mappings.hashCode();
		return result;
	}

	public int hasMapping(final String key) {
		return this.mappings.indexOf(key);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.serialization.Schema#indicesOf(eu.stratosphere.sopremo.expressions.EvaluationExpression)
	 */
	@Override
	public IntSet indicesOf(final EvaluationExpression expression) {
		final ObjectAccess objectAccess = (ObjectAccess) expression;
		final int index = this.mappings.indexOf(objectAccess.getField());
		if (index == -1)
			throw new IllegalArgumentException("Field not found " + objectAccess.getField());
		return IntSets.singleton(index);
	}

	@Override
	public PactRecord jsonToRecord(final IJsonNode value, PactRecord target, final EvaluationContext context) {
		IObjectNode others;
		if (target == null || target.getNumFields() < this.mappings.size() + 1) {
			// the last element is the field "others"
			target = new PactRecord(this.mappings.size() + 1);
			for (int i = 0; i < this.mappings.size(); i++)
				target.setField(i, new JsonNodeWrapper(MissingNode.getInstance()));
			target.setField(this.mappings.size(), new JsonNodeWrapper(others = new ObjectNode()));
		} else {
			final JsonNodeWrapper wrappedField = target.getField(target.getNumFields() - 1, JsonNodeWrapper.class);
			others = wrappedField.getValue(IObjectNode.class);
			others.clear();
			target.setField(target.getNumFields() - 1, wrappedField);
		}

		// traverse the mapping and fill them into the record
		final IObjectNode object = (IObjectNode) value;
		for (int i = 0; i < this.mappings.size(); i++) {
			final IJsonNode node = object.get(this.mappings.get(i));
			final JsonNodeWrapper wrappedField = target.getField(i, JsonNodeWrapper.class);
			wrappedField.setValue(node);
			target.setField(i, wrappedField);
		}

		// each other entry comes into the last record field
		for (final Entry<String, IJsonNode> entry : object)
			if (!this.mappings.contains(entry.getKey()))
				others.put(entry.getKey(), entry.getValue());

		return target;
	}

	@Override
	public IJsonNode recordToJson(final PactRecord record, final IJsonNode target) {
		if (this.mappings.size() + 1 != record.getNumFields())
			throw new IllegalStateException("Schema does not match to record!");

		final IObjectNode targetObject;
		if (target == null)
			targetObject = new ObjectNode();
		else {
			targetObject = (IObjectNode) target;
			targetObject.clear();
		}

		for (int i = 0; i < this.mappings.size(); i++)
			if (record.getField(i, JsonNodeWrapper.class) != null)
				targetObject.put(this.mappings.get(i), record.getField(i, JsonNodeWrapper.class).getValue());

		targetObject.putAll(record.getField(this.mappings.size(), JsonNodeWrapper.class).getValue(IObjectNode.class));

		return targetObject;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder("ObjectSchema [");
		for (int index = 0; index < this.mappings.size(); index++)
			builder.append(this.mappings.get(index)).append(", ");
		builder.append("<other>]");
		return builder.toString();
	}
}
