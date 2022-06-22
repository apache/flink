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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.annotation.Experimental;

import org.apache.commons.lang3.ArrayUtils;

import java.io.Serializable;

/** A data class that model parameterized sql predicate. */
@Experimental
public class ParameterizedPredicate {
    private String predicate;
    private Serializable[] parameters;

    public ParameterizedPredicate(String predicate) {
        this.predicate = predicate;
        this.parameters = new Serializable[0];
    }

    public Serializable[] getParameters() {
        return parameters;
    }

    public void setParameters(Serializable[] parameters) {
        this.parameters = parameters;
    }

    public String getPredicate() {
        return predicate;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }

    public ParameterizedPredicate combine(String operator, ParameterizedPredicate that) {
        this.predicate = String.format("(%s %s %s)", this.predicate, operator, that.predicate);
        this.parameters = ArrayUtils.addAll(this.parameters, that.parameters);
        return this;
    }
}
