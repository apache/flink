/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.kinesis.test.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/** POJO model class for sending and receiving records on Kinesis during e2e test. */
public class Order {
    private final String code;
    private final int quantity;

    public Order(@JsonProperty("code") final String code, @JsonProperty("quantity") int quantity) {
        this.code = code;
        this.quantity = quantity;
    }

    public String getCode() {
        return code;
    }

    public int getQuantity() {
        return quantity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Order order = (Order) o;
        return quantity == order.quantity && Objects.equals(code, order.code);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, quantity);
    }

    @Override
    public String toString() {
        return "Order{" + "code='" + code + '\'' + ", quantity=" + quantity + '}';
    }
}
