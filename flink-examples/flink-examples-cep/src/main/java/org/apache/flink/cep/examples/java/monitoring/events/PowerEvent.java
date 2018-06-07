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

package org.apache.flink.cep.examples.java.monitoring.events;

import java.util.Objects;

/**
 * Power event.
 */
public class PowerEvent extends MonitoringEvent {

	private double voltage;

	public PowerEvent() {
		this(-1, -1);
	}

	public PowerEvent(int rackID, double voltage) {
		super(rackID);
		this.voltage = voltage;
	}

	public double getVoltage() {
		return voltage;
	}

	public void setVoltage(double voltage) {
		this.voltage = voltage;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}

		PowerEvent that = (PowerEvent) o;

		return Double.compare(that.voltage, voltage) == 0;
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), voltage);
	}

	@Override
	public String toString() {
		return "PowerEvent(" + getRackID() + ", " + voltage + ")";
	}

}
