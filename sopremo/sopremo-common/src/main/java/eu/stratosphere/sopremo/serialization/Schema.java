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
package eu.stratosphere.sopremo.serialization;

import java.io.Serializable;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.testing.SchemaUtils;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
public interface Schema extends Serializable {

	public static Schema Default = new Default();

	/**
	 * @return
	 */
	public Class<? extends Value>[] getPactSchema();

	/**
	 * @param value
	 * @return
	 */
	public PactRecord jsonToRecord(IJsonNode value, PactRecord target);

	public IJsonNode recordToJson(PactRecord record, IJsonNode target);

	public static class Default implements Schema {
		/**
		 * 
		 */
		private static final long serialVersionUID = 4142913511513235355L;

		private static final Class<? extends Value>[] PactSchema = SchemaUtils.combineSchema(JsonNodeWrapper.class,
			JsonNodeWrapper.class);

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.type.Schema#getPactSchema()
		 */
		@Override
		public Class<? extends Value>[] getPactSchema() {
			return PactSchema;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.type.Schema#jsonToRecord(eu.stratosphere.sopremo.type.IJsonNode,
		 * eu.stratosphere.pact.common.type.PactRecord)
		 */
		@Override
		public PactRecord jsonToRecord(final IJsonNode value, PactRecord target) {
			if (target == null)
				target = new PactRecord(2);
			// target.setField(0, new PactInteger(value.getType().ordinal()));
			target.setField(0, SopremoUtil.wrap(((ArrayNode) value).get(0)));
			target.setField(1, SopremoUtil.wrap(((ArrayNode) value).get(1)));
			return target;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.type.Schema#recordToJson(eu.stratosphere.pact.common.type.PactRecord,
		 * eu.stratosphere.sopremo.type.IJsonNode)
		 */
		@Override
		public IJsonNode recordToJson(final PactRecord record, final IJsonNode target) {
			final JsonNodeWrapper key = record.getField(0, JsonNodeWrapper.class);
			final JsonNodeWrapper value = record.getField(1, JsonNodeWrapper.class);
			return JsonUtil.asArray(key.getValue(), value.getValue());
			// IJsonNode.Type type = IJsonNode.Type.values()[record.getField(0, JsonNodeWrapper.class).getValue()];
			// if (target == null || target.getType() != type)
			// target = InstantiationUtil.instantiate(type.getClazz(), IJsonNode.class);
			// record.getFieldInto(1, target);
			// return target;
		}
	}
}
