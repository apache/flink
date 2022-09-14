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

package org.apache.flink.cep;

/** A subclass of {@link Event} for usage in tests. */
public class SubEvent extends Event {
    private final double volume;

    public SubEvent(int id, String name, double price, double volume) {
        super(id, name, price);
        this.volume = volume;
    }

    public double getVolume() {
        return volume;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SubEvent && super.equals(obj) && ((SubEvent) obj).volume == volume;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + (int) volume;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append("SubEvent(")
                .append(getId())
                .append(", ")
                .append(getName())
                .append(", ")
                .append(getPrice())
                .append(", ")
                .append(getVolume())
                .append(")");

        return builder.toString();
    }
}
