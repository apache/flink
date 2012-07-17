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

import static eu.stratosphere.sopremo.pact.IOConstants.ENCODING;
import static eu.stratosphere.sopremo.pact.IOConstants.SCHEMA;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.io.JsonGenerator;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Writes json files with Jackson. The incoming key/value pair consists of {@link PactNull} and a {@link IJsonNode} .
 * 
 * @author Arvid Heise
 */
public class JsonOutputFormat extends FileOutputFormat {

	private JsonGenerator generator;

	private IJsonNode node;

	private Schema schema;

	private Charset encoding;

	@Override
	public void close() throws IOException {
		this.generator.writeEndArray();
		this.generator.close();
		super.close();
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.common.io.FileOutputFormat#configure(eu.stratosphere.nephele.configuration.Configuration)
	 */
	@Override
	public void configure(final Configuration parameters) {
		super.configure(parameters);
		this.schema = SopremoUtil.deserialize(parameters, SCHEMA, Schema.class);
		if (this.schema == null)
			throw new IllegalStateException("Could not deserialize input schema");
		this.encoding = Charset.forName(parameters.getString(ENCODING, "utf-8"));
	}

	@Override
	public void open(final int taskNumber) throws IOException {
		super.open(taskNumber);

		this.generator = new JsonGenerator(new OutputStreamWriter(this.stream, this.encoding));
		this.generator.writeStartArray();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.OutputFormat#writeRecord(eu.stratosphere.pact.common.type.PactRecord)
	 */
	@Override
	public void writeRecord(final PactRecord record) throws IOException {
		this.generator.writeTree(this.node = this.schema.recordToJson(record, this.node));
	}

}
