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
package eu.stratosphere.pact.common.type.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import eu.stratosphere.pact.common.type.Value;

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
}