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

public class TemperatureEvent extends MonitoringEvent {
    private double temperature;

    public TemperatureEvent(int rackID, double temperature) {
        super(rackID);

        this.temperature = temperature;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TemperatureEvent) {
            TemperatureEvent other = (TemperatureEvent) obj;

            return other.canEquals(this) && super.equals(other) && temperature == other.temperature;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 41 * super.hashCode() + Double.hashCode(temperature);
    }

    @Override
    public boolean canEquals(Object obj){
        return obj instanceof TemperatureEvent;
    }

    @Override
    public String toString() {
        return "TemperatureEvent(" + getRackID() + ", " + temperature + ")";
    }
}
