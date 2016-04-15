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

import java.util.HashMap;
import java.util.Map;

/**
 * Represents media elements uploaded with the {@link org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet}.
 */
public class Media {


	private String display_url = "";

	private String expanded_url = "";

	private long id;

	private String id_str = "";

	private long[] indices;

	private String media_url = "";

	private String media_url_https = "";

	private Map<String, Size> sizes;

	private String type = "";

	private String url = "";

	public Media() {

		this.display_url = "";
		this.expanded_url = "";
		this.id = 0L;
		this.id_str = "";
		this.setIndices(new long[]{0L, 0L});
		this.media_url = "";
		this.media_url_https = "";
		this.sizes = new HashMap<String, Size>();
		this.type = "";
		this.url = "";

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

	public long[] getIndices() {
		return indices;
	}

	public void setIndices(long[] indices) {
		this.indices = indices;
	}

	public String getMedia_url() {
		return media_url;
	}

	public void setMedia_url(String media_url) {
		this.media_url = media_url;
	}

	public String getMedia_url_https() {
		return media_url_https;
	}

	public void setMedia_url_https(String media_url_https) {
		this.media_url_https = media_url_https;
	}

	public Map<String, Size> getSizes() {
		return sizes;
	}

	public void setSizes(Map<String, Size> sizes) {
		this.sizes = sizes;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
}
