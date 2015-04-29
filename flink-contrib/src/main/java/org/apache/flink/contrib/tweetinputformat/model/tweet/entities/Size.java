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
 * An object showing available sizes for the media file.
 */
public class Size {

	private long w;

	private long h;

	private String resize = "";


	public Size(long width, long height, String resize) {

		this.w = width;
		this.h = height;
		this.resize = resize;

	}


	public long getWidth() {
		return w;
	}

	public void setWidth(long width) {
		this.w = width;
	}

	public long getHeight() {
		return h;
	}

	public void setHeight(long height) {
		this.h = height;
	}

	public String getResize() {
		return resize;
	}

	public void setResize(String resize) {
		this.resize = resize;
	}
}
