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

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.OutputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactNull;

/**
 * Writes json files with Jackson. The incoming key/value pair consists of {@link PactNull} and a {@link PactJsonObject}
 * .
 * 
 * @author Arvid Heise
 */
public class JsonOutputFormat extends OutputFormat<PactNull, PactJsonObject> {

	private JsonEncoding encoding = JsonEncoding.UTF8;

	private JsonGenerator generator;

	@Override
	public void close() throws IOException {
		this.generator.writeEndArray();
		this.generator.close();
	}

	@Override
	public void configure(final Configuration parameters) {
		final String encoding = parameters.getString(PARAMETER_ENCODING, null);
		if (encoding != null)
			this.encoding = JsonEncoding.valueOf(encoding);
	}

	@Override
	protected void initTypes() {
		this.ok = PactNull.class;
		this.ov = PactJsonObject.class;
	}

	@Override
	public void open() throws IOException {
		this.generator = new JsonFactory().createJsonGenerator(this.stream, this.encoding);
		this.generator.setCodec(new ObjectMapper());
		this.generator.writeStartArray();
	}

	@Override
	public void writePair(final KeyValuePair<PactNull, PactJsonObject> pair) throws JsonProcessingException,
			IOException {
		this.generator.writeTree(pair.getValue().getValue());
	}

	private static final String PARAMETER_ENCODING = "Encoding";

}
