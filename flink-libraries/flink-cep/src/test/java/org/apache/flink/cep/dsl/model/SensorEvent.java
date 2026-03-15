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

package org.apache.flink.cep.dsl.model;

import java.io.Serializable;
import java.util.Objects;

/** IoT sensor event model for DSL testing. */
public class SensorEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String sensorId;
    private String eventType;
    private double value;
    private String unit;
    private long timestamp;
    private String location;
    private String status;

    public SensorEvent() {}

    public SensorEvent(
            String sensorId,
            String eventType,
            double value,
            String unit,
            long timestamp,
            String location,
            String status) {
        this.sensorId = sensorId;
        this.eventType = eventType;
        this.value = value;
        this.unit = unit;
        this.timestamp = timestamp;
        this.location = location;
        this.status = status;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SensorEvent that = (SensorEvent) o;
        return Double.compare(that.value, value) == 0
                && timestamp == that.timestamp
                && Objects.equals(sensorId, that.sensorId)
                && Objects.equals(eventType, that.eventType)
                && Objects.equals(unit, that.unit)
                && Objects.equals(location, that.location)
                && Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorId, eventType, value, unit, timestamp, location, status);
    }

    @Override
    public String toString() {
        return "SensorEvent{"
                + "sensorId='"
                + sensorId
                + '\''
                + ", eventType='"
                + eventType
                + '\''
                + ", value="
                + value
                + ", unit='"
                + unit
                + '\''
                + ", timestamp="
                + timestamp
                + ", location='"
                + location
                + '\''
                + ", status='"
                + status
                + '\''
                + '}';
    }
}
