/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.tweetinputformat.io;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet;
import org.apache.flink.core.fs.FileInputSplit;
import org.codehaus.jackson.JsonParseException;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;


public class SimpleTweetInputFormat extends DelimitedInputFormat<Tweet> implements ResultTypeQueryable<Tweet> {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleTweetInputFormat.class);

	private transient JSONParser parser;
	private transient TweetHandler handler;

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		this.handler = new TweetHandler();
		this.parser = new JSONParser();
	}

	@Override
	public Tweet nextRecord(Tweet record) throws IOException {
		Boolean result = false;

		do {
			try {
				record.reset(0);
				record = super.nextRecord(record);
				result = true;

			} catch (JsonParseException e) {
				result = false;

			}
		} while (!result);

		return record;
	}

	@Override
	public Tweet readRecord(Tweet reuse, byte[] bytes, int offset, int numBytes) throws IOException {

		InputStreamReader jsonReader = new InputStreamReader(new ByteArrayInputStream(bytes));
		jsonReader.skip(offset);

		try {

			handler.reuse = reuse;
			parser.parse(jsonReader, handler, false);
		} catch (ParseException e) {

			LOG.debug(" Tweet Parsing Exception : " + e.getMessage());
		}

		return reuse;
	}

	@Override
	public TypeInformation<Tweet> getProducedType() {
		return new GenericTypeInfo<Tweet>(Tweet.class);
	}
}