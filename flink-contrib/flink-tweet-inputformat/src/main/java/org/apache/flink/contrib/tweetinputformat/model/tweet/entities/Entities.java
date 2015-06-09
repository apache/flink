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

import java.util.ArrayList;
import java.util.List;

/**
 * Entities which have been parsed out of the text of the
 * {@link package org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet}.
 */
public class Entities {

	private List<HashTags> hashtags;

	private List<Media> media;

	private List<URL> urls;

	private List<UserMention> user_mentions;

	private List<Symbol> symbols;

	public Entities() {

		hashtags = new ArrayList<HashTags>();
		media = new ArrayList<Media>();
		urls = new ArrayList<URL>();
		user_mentions = new ArrayList<UserMention>();
		symbols = new ArrayList<Symbol>();

	}

	public List<HashTags> getHashtags() {
		return hashtags;
	}

	public void setHashtags(List<HashTags> hashtags) {
		this.hashtags = hashtags;
	}

	public List<Media> getMedia() {
		return media;
	}

	public void setMedia(List<Media> media) {
		this.media = media;
	}

	public List<URL> getUrls() {
		return urls;
	}

	public void setUrls(List<URL> urls) {
		this.urls = urls;
	}

	public List<UserMention> getUser_mentions() {
		return user_mentions;
	}

	public void setUser_mentions(List<UserMention> user_mentions) {
		this.user_mentions = user_mentions;
	}


	public List<Symbol> getSymbols() {
		return symbols;
	}

	public void setSymbols(List<Symbol> symbols) {
		this.symbols = symbols;
	}

}
