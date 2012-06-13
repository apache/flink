package eu.stratosphere.sopremo.serialization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.ObjectNode;

public class ObjectSchema implements Schema {

	/**
	 * @author Michael Hopstock
	 * @author Tommy Neubert
	 */
	private static final long serialVersionUID = 4037447354469753483L;

	private List<String> mappings = new ArrayList<String>();

	/**
	 * Initializes ObjectSchema.
	 */
	public ObjectSchema() {
	}

	public ObjectSchema(String... mappings) {
		for (String mapping : mappings)
			this.mappings.add(mapping);
	}

	@Override
	public Class<? extends Value>[] getPactSchema() {
		@SuppressWarnings("unchecked")
		Class<? extends Value>[] schema = new Class[this.mappings.size() + 1];

		for (int i = 0; i <= this.mappings.size(); i++)
			schema[i] = JsonNodeWrapper.class;

		return schema;
	}

	public List<String> getMappings() {
		return this.mappings;
	}

	/**
	 * Sets the mapping to the specified value.
	 * 
	 * @param mapping
	 *        the mapping to set
	 */
	public void setMappings(Iterable<String> mappings) {
		if (mappings == null)
			throw new NullPointerException("mapping must not be null");

		this.mappings.clear();
		for (String mapping : mappings)
			this.mappings.add(mapping);
	}

	/**
	 * @param schema
	 *        the keys, that should be extracted from the {@link ObjectNode} and saved into the first fields of
	 *        {@link PactRecord}
	 */
	public void setMappings(String... schema) {
		this.setMappings(Arrays.asList(schema));
	}

	public void setMappingsWithAccesses(Iterable<ObjectAccess> schema) {
		List<String> mappings = new ArrayList<String>();
		for (ObjectAccess objectAccess : schema)
			mappings.add(objectAccess.getField());
		this.setMappings(mappings);
	}

	public int hasMapping(String key) {
		return this.mappings.indexOf(key);
	}

	public int getMappingSize() {
		return this.mappings.size();
	}

	@Override
	public PactRecord jsonToRecord(IJsonNode value, PactRecord target, EvaluationContext context) {
		IObjectNode others;
		if (target == null || target.getNumFields() < this.mappings.size() + 1) {
			// the last element is the field "others"
			target = new PactRecord(this.mappings.size() + 1);
			for (int i = 0; i < this.mappings.size(); i++)
				target.setField(i, new JsonNodeWrapper(MissingNode.getInstance()));
			target.setField(this.mappings.size(), new JsonNodeWrapper(others = new ObjectNode()));
		} else {
			JsonNodeWrapper wrappedField = target.getField(target.getNumFields() - 1, JsonNodeWrapper.class);
			others = wrappedField.getValue(IObjectNode.class);
			others.removeAll();
			target.setField(target.getNumFields() - 1, wrappedField);
		}

		// traverse the mapping and fill them into the record
		IObjectNode object = (IObjectNode) value;
		for (int i = 0; i < this.mappings.size(); i++) {
			IJsonNode node = object.get(this.mappings.get(i));
			JsonNodeWrapper wrappedField = target.getField(i, JsonNodeWrapper.class);
			wrappedField.setValue(node);
			target.setField(i, wrappedField);
		}

		// each other entry comes into the last record field
		for (Entry<String, IJsonNode> entry : object.getEntries()) 
			if (!this.mappings.contains(entry.getKey())) 
				others.put(entry.getKey(), entry.getValue());

		return target;
	}

	@Override
	public IJsonNode recordToJson(PactRecord record, IJsonNode target) {
		if (this.mappings.size() + 1 != record.getNumFields())
			throw new IllegalStateException("Schema does not match to record!");

		final IObjectNode targetObject;
		if (target == null)
			targetObject = new ObjectNode();
		else {
			targetObject = (IObjectNode) target;
			targetObject.removeAll();
		}

		for (int i = 0; i < this.mappings.size(); i++)
			if (record.getField(i, JsonNodeWrapper.class) != null)
				targetObject.put(this.mappings.get(i), record.getField(i, JsonNodeWrapper.class).getValue());

		targetObject.putAll(record.getField(this.mappings.size(), JsonNodeWrapper.class).getValue(IObjectNode.class));

		return targetObject;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.serialization.Schema#indicesOf(eu.stratosphere.sopremo.expressions.EvaluationExpression)
	 */
	@Override
	public int[] indicesOf(EvaluationExpression expression) {
		ObjectAccess objectAccess = (ObjectAccess) expression;
		int index = this.mappings.indexOf(objectAccess.getField());
		if (index == -1)
			throw new IllegalArgumentException("Field not found " + objectAccess.getField());
		return new int[] { index };
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.mappings.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		ObjectSchema other = (ObjectSchema) obj;
		return this.mappings.equals(other.mappings);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder("ObjectSchema [");
		for (int index = 0; index < this.mappings.size(); index++)
			builder.append(this.mappings.get(index)).append(", ");
		builder.append("<other>]");
		return builder.toString();
	}
}
