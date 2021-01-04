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

package org.apache.flink.graph.drivers.parameter;

import org.apache.flink.api.java.utils.ParameterTool;

/** A {@link Parameter} storing a {@link String}. */
public class StringParameter extends SimpleParameter<String> {

    /**
     * Set the parameter name and add this parameter to the list of parameters stored by owner.
     *
     * @param owner the {@link Parameterized} using this {@link Parameter}
     * @param name the parameter name
     */
    public StringParameter(ParameterizedBase owner, String name) {
        super(owner, name);
    }

    @Override
    public StringParameter setDefaultValue(String defaultValue) {
        super.setDefaultValue(defaultValue);
        return this;
    }

    @Override
    public void configure(ParameterTool parameterTool) {
        value =
                hasDefaultValue
                        ? parameterTool.get(name, defaultValue)
                        : parameterTool.getRequired(name);
    }

    @Override
    public String toString() {
        return value;
    }
}
