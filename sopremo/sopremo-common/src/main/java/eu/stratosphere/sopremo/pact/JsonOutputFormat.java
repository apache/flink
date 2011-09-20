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

import java.io.IOException;
import java.nio.charset.Charset;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.sopremo.io.JsonGenerator;
import eu.stratosphere.sopremo.jsondatamodel.ObjectMapper;

/**
 * Writes json files with Jackson. The incoming key/value pair consists of {@link PactNull} and a {@link PactJsonObject}
 * .
 * 
 * @author Arvid Heise
 */
public class JsonOutputFormat extends FileOutputFormat<PactJsonObject.Key, PactJsonObject> {

	private Charset encoding;

	private JsonGenerator generator;

	public static final String PARAMETER_ENCODING = "Encoding";

	public JsonOutputFormat() {
		this.keyClass = PactJsonObject.Key.class;
		this.valueClass = PactJsonObject.class;
	}

	@Override
	public void close() throws IOException {
		this.generator.writeEndArray();
		this.generator.close();
		super.close();
	}

	@Override
	public void configure(final Configuration parameters) {
		super.configure(parameters);

		final String encoding = parameters.getString(PARAMETER_ENCODING, null);
		if (encoding != null)
			this.encoding = Charset.forName(encoding);
		else
			this.encoding = Charset.forName("UTF8");
	}

	@Override
	public void open(final int taskNumber) throws IOException {
		super.open(taskNumber);

		this.generator = new JsonGenerator(this.stream, this.encoding);
		this.generator.writeStartArray();
	}

	@Override
	public void writeRecord(final KeyValuePair<PactJsonObject.Key, PactJsonObject> pair) throws IOException {
		this.generator.writeTree(pair.getValue().getValue());
	}

}
