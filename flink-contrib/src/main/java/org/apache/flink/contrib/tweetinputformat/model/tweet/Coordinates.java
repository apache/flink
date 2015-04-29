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
 * Nullable. Represents the geographic location of this
 * {@link org.apache.flink.contrib.tweetinputformat.model.tweet.Tweet} as reported by the user or client
 * application. The inner coordinates array is formatted as geoJSON longitude first, then latitude)
 */
public class Coordinates {

	private String type = "point";

	private double[] coordinates = new double[2];

	public Coordinates() {

	}

	public double[] getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(double[] coordinates) {
		this.coordinates = coordinates;
	}

	public void setCoordinates(double longitude, double latitude) {
		this.coordinates[0] = longitude;
		this.coordinates[1] = latitude;
	}

	public String getType() {
		return type;
	}

	@Override
	public String toString() {
		return "longitude = " + this.coordinates[0] + "  latitude = " + this.coordinates[1];
	}
}
