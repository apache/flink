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

package org.apache.flink.contrib.tweetinputformat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.tweetinputformat.io.SimpleTweetInputFormat;
import org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet;
import org.apache.flink.contrib.tweetinputformat.model.tweet.entities.HashTags;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class SimpleTweetInputFormatTest {

	private Tweet tweet;

	private SimpleTweetInputFormat simpleTweetInputFormat;

	private FileInputSplit fileInputSplit;

	protected Configuration config;

	protected File tempFile;


	@Before
	public void testSetUp() {


		simpleTweetInputFormat = new SimpleTweetInputFormat();

		File jsonFile = new File("src/main/resources/HashTagTweetSample.json");

		fileInputSplit = new FileInputSplit(0, new Path(jsonFile.getPath()), 0, jsonFile.length(), new String[]{"localhost"});
	}

	@Test
	public void testTweetInput() throws Exception {


		simpleTweetInputFormat.open(fileInputSplit);
		List<String> result;

		int i = 0;
		while (i < 4) {
			i++;
			tweet = new Tweet();
			tweet = simpleTweetInputFormat.nextRecord(tweet);

			if (tweet != null) {
				result = new ArrayList<String>();
				for (Iterator<HashTags> iterator = tweet.getEntities().getHashtags().iterator(); iterator.hasNext(); ) {
					result.add(iterator.next().getText());
				}

				if (tweet.getId_str().equals("100000000000000000")) {
					Assert.assertArrayEquals(new String[]{"example", "tweet"}, result.toArray());
				} else if (tweet.getId_str().equals("200000000000000000")) {
					Assert.assertArrayEquals(new String[]{"example", "tweet"}, result.toArray());
				} else if (tweet.getId_str().equals("300000000000000000")) {
					Assert.assertArrayEquals(new String[]{"last", "example", "that"}, result.toArray());
				} else if (tweet.getId_str().equals("400000000000000000")) {
					Assert.assertArrayEquals(new String[]{"d12", "how_to"}, result.toArray());
				}
			}
		}

		tweet = new Tweet();
		tweet = simpleTweetInputFormat.nextRecord(tweet);
		Assert.assertNull(tweet);
		Assert.assertTrue(simpleTweetInputFormat.reachedEnd());

	}
}
