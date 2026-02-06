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

/** Stock trading event model for DSL testing. */
public class StockEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String symbol;
    private String eventType;
    private double price;
    private long volume;
    private long timestamp;
    private String exchange;
    private double change;

    public StockEvent() {}

    public StockEvent(
            String symbol,
            String eventType,
            double price,
            long volume,
            long timestamp,
            String exchange,
            double change) {
        this.symbol = symbol;
        this.eventType = eventType;
        this.price = price;
        this.volume = volume;
        this.timestamp = timestamp;
        this.exchange = exchange;
        this.change = change;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public long getVolume() {
        return volume;
    }

    public void setVolume(long volume) {
        this.volume = volume;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public double getChange() {
        return change;
    }

    public void setChange(double change) {
        this.change = change;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StockEvent that = (StockEvent) o;
        return Double.compare(that.price, price) == 0
                && volume == that.volume
                && timestamp == that.timestamp
                && Double.compare(that.change, change) == 0
                && Objects.equals(symbol, that.symbol)
                && Objects.equals(eventType, that.eventType)
                && Objects.equals(exchange, that.exchange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbol, eventType, price, volume, timestamp, exchange, change);
    }

    @Override
    public String toString() {
        return "StockEvent{"
                + "symbol='"
                + symbol
                + '\''
                + ", eventType='"
                + eventType
                + '\''
                + ", price="
                + price
                + ", volume="
                + volume
                + ", timestamp="
                + timestamp
                + ", exchange='"
                + exchange
                + '\''
                + ", change="
                + change
                + '}';
    }
}
