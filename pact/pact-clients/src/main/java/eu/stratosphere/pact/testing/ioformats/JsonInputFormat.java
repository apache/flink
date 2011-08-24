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
package eu.stratosphere.pact.testing.ioformats;

import java.io.IOException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.pact.common.io.input.FileInputFormat;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactJsonObject;
import eu.stratosphere.pact.common.type.base.PactLong;

/**
 * Reads json files with Jackson. The resulting key/value pair consists of an id and a {@link PactJsonObject}. The id is
 * an auto-incremented {@link PactLong}.
 * 
 * @author Arvid Heise
 */
public class JsonInputFormat extends FileInputFormat<PactLong, PactJsonObject> {

	private boolean array;

	private long currentId;

	private boolean end;

	private JsonParser parser;

	
	
	
	private void checkEnd() throws IOException, JsonParseException {
		if (this.array && this.parser.nextToken() == JsonToken.END_ARRAY || !this.array
			&& this.parser.nextToken() == null)
			this.end = true;
	}

	@Override
	public void close() throws IOException {
		super.close();
		this.parser.close();
	}

	@Override
	public void configure(final Configuration parameters) {
		super.configure(parameters);
	}

	@Override
	public KeyValuePair<PactLong, PactJsonObject> createPair() {
		return new KeyValuePair<PactLong, PactJsonObject>(new PactLong(), new PactJsonObject());
	}

	@Override
	public boolean nextRecord(final KeyValuePair<PactLong, PactJsonObject> pair) throws JsonProcessingException,
			IOException
	{
		if (!this.end) {
			pair.getKey().setValue(this.currentId++);
			pair.getValue().setValue(this.parser.readValueAsTree());
			checkEnd();
			return true;
		}

		return false;
	}

	@Override
	public void open(FileInputSplit split) throws JsonParseException, IOException {
		super.open(split);
		
		this.end = false;
		this.currentId = 0;
		this.parser = new JsonFactory().createJsonParser(this.stream);
		this.parser.setCodec(new ObjectMapper());
		if (this.array = this.parser.nextToken() == JsonToken.START_ARRAY)
			this.parser.clearCurrentToken();
		checkEnd();
	}

	@Override
	public boolean reachedEnd() {
		return this.end;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.io.InputFormat#getStatistics()
	 */
	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return null;
	}

}