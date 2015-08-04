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
package org.apache.flink.contrib.tweetinputformat.model.places;

/**
 * {@link org.apache.flink.contrib.tweetinputformat.model.places.Places} are specific, named locations with
 * corresponding geo {@link org.apache.flink.contrib.tweetinputformat.model.tweet.Coordinates}. They can be attached
 * to {@link org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet} by specifying a place_id when tweeting. <br>
 * {@link org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet} associated with places are not necessarily
 * issued from that location but could also potentially be about that location.<br>
 * {@link org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet} can be searched for. Tweets can also be found
 * by place_id.
 */
public class Places {


	private Attributes attributes;

	private BoundingBox bounding_box;

	private String country = "";

	private String country_code = "";

	private String full_name = "";

	private String id = "";

	private String name = "";

	private String place_type = "";

	private String url = "";


	public Places() {
		attributes = new Attributes();
		bounding_box = new BoundingBox();

	}

	public Attributes getAttributes() {
		return attributes;
	}

	public void setAttributes(Attributes attributes) {
		this.attributes = attributes;
	}

	public BoundingBox getBounding_box() {
		return bounding_box;
	}

	public void setBounding_box(BoundingBox bounding_box) {
		this.bounding_box = bounding_box;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getCountry_code() {
		return country_code;
	}

	public void setCountry_code(String country_code) {
		this.country_code = country_code;
	}

	public String getFull_name() {
		return full_name;
	}

	public void setFull_name(String full_name) {
		this.full_name = full_name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPlace_type() {
		return place_type;
	}

	public void setPlace_type(String place_type) {
		this.place_type = place_type;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}


}
