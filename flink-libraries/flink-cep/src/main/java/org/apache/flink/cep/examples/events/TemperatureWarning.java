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

package org.apache.flink.cep.examples.events;

public class TemperatureWarning {

	private int rackID;
	private double averageTemperature;

	public TemperatureWarning(int rackID, double averageTemperature) {
		this.rackID = rackID;
		this.averageTemperature = averageTemperature;
	}

	public TemperatureWarning() {
		this(-1, -1);
	}

	public int getRackID() {
		return rackID;
	}

	public void setRackID(int rackID) {
		this.rackID = rackID;
	}

	public double getAverageTemperature() {
		return averageTemperature;
	}

	public void setAverageTemperature(double averageTemperature) {
		this.averageTemperature = averageTemperature;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TemperatureWarning) {
			TemperatureWarning other = (TemperatureWarning) obj;

			return rackID == other.rackID && averageTemperature == other.averageTemperature;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return 41 * rackID + Double.hashCode(averageTemperature);
	}

	@Override
	public String toString() {
		return "TemperatureWarning(" + getRackID() + ", " + averageTemperature + ")";
	}
}
