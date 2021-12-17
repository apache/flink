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

/** A {@link Parameter} with a default value. */
public abstract class SimpleParameter<T> implements Parameter<T> {

    protected final String name;

    protected boolean hasDefaultValue = false;

    protected T defaultValue;

    protected T value;

    /**
     * Set the parameter name and add this parameter to the list of parameters stored by owner.
     *
     * @param owner the {@link Parameterized} using this {@link Parameter}
     * @param name the parameter name
     */
    protected SimpleParameter(ParameterizedBase owner, String name) {
        this.name = name;
        owner.addParameter(this);
    }

    /**
     * Set the default value, used if no value is set by the command-line configuration.
     *
     * @param defaultValue the default value
     * @return this
     */
    protected SimpleParameter setDefaultValue(T defaultValue) {
        this.hasDefaultValue = true;
        this.defaultValue = defaultValue;

        return this;
    }

    @Override
    public String getUsage() {
        String option = "--" + name + " " + name.toUpperCase();

        return hasDefaultValue ? "[" + option + "]" : option;
    }

    @Override
    public boolean isHidden() {
        return name.startsWith("__");
    }

    @Override
    public T getValue() {
        return value;
    }
}
