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

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.sopremo.JsonUtil;

/**
 * Reads json files with Jackson. The resulting key/value pair consists of an id and a {@link PactJsonObject}. The id is
 * an auto-incremented {@link PactLong}.
 * 
 * @author Arvid Heise
 */
public class JsonInputFormat extends InputFormat<PactJsonObject.Key, PactJsonObject> {

	private boolean array;

	private boolean end;

	private JsonParser parser;

	private void checkEnd() throws IOException, JsonParseException {
		if (this.array && this.parser.nextToken() == JsonToken.END_ARRAY || !this.array
			&& this.parser.nextToken() == null)
			this.end = true;
	}

	@Override
	public void close() throws IOException {
		this.parser.close();
	}

	@Override
	public void configure(final Configuration parameters) {
	}

	@Override
	public KeyValuePair<PactJsonObject.Key, PactJsonObject> createPair() {
		return new KeyValuePair<PactJsonObject.Key, PactJsonObject>(PactJsonObject.Key.NULL,
			new PactJsonObject());
	}

	@Override
	protected void initTypes() {
		this.ok = PactJsonObject.Key.class;
		this.ov = PactJsonObject.class;
	}

	@Override
	public boolean nextPair(final KeyValuePair<PactJsonObject.Key, PactJsonObject> pair)
			throws JsonProcessingException,
			IOException {

		if (!this.end) {
			pair.getValue().setValue(this.parser.readValueAsTree());
			this.checkEnd();
			return true;
		}

		return false;
	}

	@Override
	public void open() throws JsonParseException, IOException {
		this.end = false;
		this.parser = JsonUtil.FACTORY.createJsonParser(this.stream);
		this.parser.setCodec(JsonUtil.OBJECT_MAPPER);
		if (this.array = this.parser.nextToken() == JsonToken.START_ARRAY)
			this.parser.clearCurrentToken();
		this.checkEnd();
	}

	@Override
	public boolean reachedEnd() {
		return this.end;
	}

}