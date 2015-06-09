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
 * Nullable. An collection of brief user objects (usually only one) indicating users who contributed
 * to the authorship of the {@link org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet} on behalf of the
 * official tweet author.
 */
public class Contributors {


	private Long id = 0L;

	private String id_str = "";

	private String screenName = "";

	public Contributors() {
		reset();
	}

	public Contributors(long id, String id_str, String screenName) {

		this.id = id;
		this.id_str = id_str;
		this.screenName = screenName;
	}

	public void reset() {

		id = 0L;
		id_str = "";
		screenName = "";

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

	public void setId_str(String id_str) {
		this.id_str = id_str;
	}

	public String getScreenName() {
		return screenName;
	}

	public void setScreenName(String screenName) {
		this.screenName = screenName;
	}


}
