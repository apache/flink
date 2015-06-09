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
package org.apache.flink.contrib.tweetinputformat.model.tweet.entities;

/**
 * Represents URLs included in the text of a Tweet or within textual fields of a
 * {@link package org.apache.flink.contrib.tweetinputformat.model.tweet.User.Users} object.
 */
public class URL {

	private String url = "";

	private String display_url = "";

	private String expanded_url = "";

	private long[] indices;

	public URL() {
		this.setIndices(new long[]{0L, 0L});
	}


	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDisplay_url() {
		return display_url;
	}

	public void setDisplay_url(String display_url) {
		this.display_url = display_url;
	}

	public String getExpanded_url() {
		return expanded_url;
	}

	public void setExpanded_url(String expanded_url) {
		this.expanded_url = expanded_url;
	}

	public long[] getIndices() {
		return indices;
	}

	public void setIndices(long[] indices) {
		this.indices = indices;
	}
}
