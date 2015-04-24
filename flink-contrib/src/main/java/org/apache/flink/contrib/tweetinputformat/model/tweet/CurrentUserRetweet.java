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
package org.apache.flink.contrib.tweetinputformat.model.tweet;

/**
 * Details the {@link org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet} ID of the user’s own retweet (if
 * existent) of this {@link org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet}.
 */
public class CurrentUserRetweet {

	private long id;

	private String id_str = "";

	public CurrentUserRetweet() {
		reset();
	}

	public void reset() {
		id = 0L;
		id_str = "";

	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getId_str() {
		return id_str;
	}

	public void setId_str() {
		this.id_str = Long.toString(id);
	}
}
