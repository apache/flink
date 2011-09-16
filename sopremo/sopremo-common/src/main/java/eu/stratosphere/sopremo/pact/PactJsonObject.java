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

import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.jsondatamodel.ObjectNode;

/**
 * Wraps a Jackson {@link JsonNode}.
 * 
 * @author Arvid Heise
 */
public class PactJsonObject implements Value {
//	private final PactString serializationString = new PactString();

	private JsonNode value;

	/**
	 * Initializes PactJsonObject with an empty {@link ObjectNode}.
	 */
	public PactJsonObject() {
		this.value = JsonUtil.OBJECT_MAPPER.createObjectNode();
	}

	/**
	 * Initializes PactJsonObject with the given {@link JsonNode}.
	 * 
	 * @param value
	 *        the value that is wrapped
	 */
	public PactJsonObject(final JsonNode value) {
		this.value = value;
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

	@Override
	public void read(final DataInput in) throws IOException {
		this.value = SopremoUtil.deserializeNode(in);
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

	@Override
	public String toString() {
		return this.value.toString();
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		SopremoUtil.serializeNode(out, this.value);
	}

//	private static boolean isValidArray(final JsonNode node) {
//		for (int index = 0, size = node.size(); index < size; index++) {
//			if (node.get(index).isArray() && !isValidArray(node.get(index)))
//				return false;
//			if (!(node.get(index) instanceof ValueNode))
//				return false;
//		}
//		return true;
//	}

	/**
	 * Creates a {@link Key} from a JsonNode if it is a suitable json node for a Key.
	 * 
	 * @param node
	 *        the wrapped node
	 * @return the key wrapping the node
	 * @throws IllegalArgumentException
	 *         if the node is not of a comparable type
	 */
	public static Key keyOf(final JsonNode node) {
		return new Key(node);
		// if (node instanceof ValueNode)
		// return new Key(node);
		// if (node.isArray()) {
		// if (!isValidArray(node))
		// throw new IllegalArgumentException(node + " is not a valid key array");
		// return new Key(node);
		// }
		// throw new IllegalArgumentException(node.getClass().getSimpleName());
	}

	public static PactJsonObject valueOf(JsonNode value) {
		if (value instanceof ArrayNode && !((ArrayNode) value).isResettable()) {
			value = ArrayNode.valueOf(value.getElements(), true);
			if (SopremoUtil.LOG.isInfoEnabled())
				SopremoUtil.LOG.info(String.format("needed to materialize the stream array " + value));
		}
		return new PactJsonObject(value);
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

		public static final Key NULL = new Key(NullNode.getInstance());

		/**
		 * Serialization construction. Please do not invoke manually.
		 */
		public Key() {
			super();
		}

		Key(final JsonNode value) {
			super(value);
		}

		@Override
		public int compareTo(final eu.stratosphere.pact.common.type.Key o) {
			return JsonNodeComparator.INSTANCE.compare(this.getValue(), ((Key) o).getValue());
		}
	}
}