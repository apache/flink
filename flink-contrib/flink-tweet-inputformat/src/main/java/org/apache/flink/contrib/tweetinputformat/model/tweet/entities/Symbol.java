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
 * An array of financial symbols starting with the dollar sign extracted from the
 * {@link org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet} text.
 */

public class Symbol {

	private String text = "";

	private long[] indices;

	public Symbol() {
		this.text = "";
		this.setIndices(new long[]{0L, 0L});

	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public long[] getIndices() {
		return indices;
	}

	public void setIndices(long[] indices) {
		this.indices = indices;
	}
}
