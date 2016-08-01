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

import java.util.ArrayList;
import java.util.List;

/**
 * A series of longitude and latitude points, defining a box which will contain the Place entity
 * this bounding box is related to. Each point is an array in the form of [longitude, latitude].
 * Points are grouped into an array per bounding box. Bounding box arrays are wrapped in one
 * additional array to be compatible with the polygon notation.
 */
public class BoundingBox {

	private List<List<double[]>> coordinates = new ArrayList<List<double[]>>();

	private String type = "Polygon";

	public BoundingBox() {

	}

	public BoundingBox(List<double[]> points) {

		this.coordinates.add(points);

	}

	public List<List<double[]>> getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(List<List<double[]>> coordinates) {
		this.coordinates = coordinates;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
}
