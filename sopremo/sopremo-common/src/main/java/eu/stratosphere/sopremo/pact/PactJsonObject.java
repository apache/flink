/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.pact;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.NumericNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;
import org.codehaus.jackson.node.ValueNode;

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Wraps a Jackson {@link JsonNode}.
 * 
 * @author Arvid Heise
 */
public class PactJsonObject implements Value {
	private static JsonFactory FACTORY = new JsonFactory();

	private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private final PactString serializationString = new PactString();

	private JsonNode value;

	/**
	 * Initializes PactJsonObject with an empty {@link ObjectNode}.
	 */
	public PactJsonObject() {
		this.value = OBJECT_MAPPER.createObjectNode();
	}

	/**
	 * Initializes PactJsonObject with the given {@link JsonNode}.
	 * 
	 * @param value
	 *        the value that is wrapped
	 */
	public PactJsonObject(JsonNode value) {
		this.value = value;
	}

	@Override
	public void read(final DataInput in) throws IOException {
		this.serializationString.read(in);
		JsonParser parser = FACTORY.createJsonParser(this.serializationString.toString());
		parser.setCodec(OBJECT_MAPPER);
		this.value = parser.readValueAsTree();
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		final StringWriter writer = new StringWriter();
		JsonGenerator generator = FACTORY.createJsonGenerator(writer);
		generator.setCodec(OBJECT_MAPPER);
		generator.writeTree(this.value);
		this.serializationString.setValue(writer.toString());
		this.serializationString.write(out);
	}

	/**
	 * Returns the value.
	 * 
	 * @return the value
	 */
	public JsonNode getValue() {
		return this.value;
	}

	/**
	 * Returns the value.
	 * 
	 * @return the value
	 */
	public ObjectNode getValueAsObject() {
		return (ObjectNode) this.value;
	}

	/**
	 * Sets the value to the specified value.
	 * 
	 * @param value
	 *        the value to set
	 */
	public void setValue(final JsonNode value) {
		if (value == null)
			throw new NullPointerException("value must not be null");

		this.value = value;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 59;
		int result = 1;
		result = prime * result + this.value.hashCode();
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final PactJsonObject other = (PactJsonObject) obj;
		return this.value.equals(other.value);
	}

	@Override
	public String toString() {
		return this.value.toString();
	}

	/**
	 * A subset of possible {@link PactJsonObject}s that can be used as a PACT key.<br>
	 * Specifically no complex json object may be used since there is no inherent order defined.<br>
	 * However, (nested) arrays and simple json values may be used.<br>
	 * Please not that only keys of the same json type are comparable.
	 * 
	 * @author Arvid Heise
	 */
	public static class Key extends PactJsonObject implements eu.stratosphere.pact.common.type.Key {

		/**
		 * Serialization construction. Please do not invoke manually.
		 */
		public Key() {
			super();
		}

		Key(ValueNode value) {
			super(value);
		}

		Key(ArrayNode value) {
			super(value);
			if (isValidArray(value))
				throw new IllegalArgumentException("is not a valid array");
		}

		@Override
		public int compareTo(eu.stratosphere.pact.common.type.Key o) {
			JsonNode value1 = getValue(), value2 = ((Key) o).getValue();
			return compare(value1, value2);
		}

		private static int compare(JsonNode value1, JsonNode value2) {
			if (value1.getClass() != value2.getClass())
				throw new ClassCastException();
			if (value1 instanceof ArrayNode)
				return compareArrays(value1, value2);
			if (value1 instanceof TextNode)
				return value1.getTextValue().compareTo(value2.getTextValue());
			if (value1 instanceof BooleanNode)
				return (value1.getBooleanValue() == value2.getBooleanValue() ? 0 : (value1.getBooleanValue() ? 1 : -1));
			if (value1 instanceof NumericNode) {
				// TODO: optimize
				return value1.getDecimalValue().compareTo(value2.getDecimalValue());
			}

			return 0;
		}

		private static int compareArrays(JsonNode value1, JsonNode value2) {
			if (value1.size() != value2.size())
				return value1.size() - value2.size();
			for (int index = 0, size = value1.size(); index < size; index++) {
				int comparisonResult = compare(value1.get(index), value2.get(index));
				if (comparisonResult != 0)
					return comparisonResult;
			}
			return 0;
		}
	}

	/**
	 * Creates a {@link Key} from a JsonNode if it is a suitable json node for a Key.
	 * 
	 * @param node
	 *        the wrapped node
	 * @return the key wrapping the node
	 * @throws IllegalArgumentException
	 *         if the node is not of a comparable type
	 */
	public static Key keyOf(JsonNode node) {
		if (node instanceof ValueNode)
			return new Key((ValueNode) node);
		if (node instanceof ArrayNode)
			return new Key((ArrayNode) node);
		throw new IllegalArgumentException(node.getClass().getSimpleName());
	}

	private static boolean isValidArray(JsonNode node) {
		for (int index = 0, size = node.size(); index < size; index++) {
			if (node.get(index) instanceof ArrayNode && !isValidArray(node.get(index)))
				return false;
			if (node.get(index) instanceof ValueNode)
				return false;
		}
		return true;
	}
}