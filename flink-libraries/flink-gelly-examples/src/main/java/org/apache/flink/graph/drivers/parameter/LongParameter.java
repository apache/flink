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

/**
 * A {@link Parameter} storing a {@link Long} within <tt>min</tt> and <tt>max</tt> bounds
 * (inclusive).
 *
 * <p>Note that the default value may be outside of these bounds.
 */
public class LongParameter extends SimpleParameter<Long> {

    private boolean hasMinimumValue = false;
    private long minimumValue;

    private boolean hasMaximumValue = false;
    private long maximumValue;

    /**
     * Set the parameter name and add this parameter to the list of parameters stored by owner.
     *
     * @param owner the {@link Parameterized} using this {@link Parameter}
     * @param name the parameter name
     */
    public LongParameter(ParameterizedBase owner, String name) {
        super(owner, name);
    }

    /**
     * Set the default value.
     *
     * <p>The default may set to any value and is not restricted by setting the minimum or maximum
     * values.
     *
     * @param defaultValue the default value.
     * @return this
     */
    public LongParameter setDefaultValue(long defaultValue) {
        super.setDefaultValue(defaultValue);

        return this;
    }

    /**
     * Set the minimum value.
     *
     * <p>If a maximum value has been set then the minimum value must not be greater than the
     * maximum value.
     *
     * @param minimumValue the minimum value
     * @return this
     */
    public LongParameter setMinimumValue(long minimumValue) {
        if (hasMaximumValue) {
            Util.checkParameter(
                    minimumValue <= maximumValue,
                    "Minimum value ("
                            + minimumValue
                            + ") must be less than or equal to maximum ("
                            + maximumValue
                            + ")");
        }

        this.hasMinimumValue = true;
        this.minimumValue = minimumValue;

        return this;
    }

    /**
     * Set the maximum value.
     *
     * <p>If a minimum value has been set then the maximum value must not be less than the minimum
     * value.
     *
     * @param maximumValue the maximum value
     * @return this
     */
    public LongParameter setMaximumValue(long maximumValue) {
        if (hasMinimumValue) {
            Util.checkParameter(
                    maximumValue >= minimumValue,
                    "Maximum value ("
                            + maximumValue
                            + ") must be greater than or equal to minimum ("
                            + minimumValue
                            + ")");
        }

        this.hasMaximumValue = true;
        this.maximumValue = maximumValue;

        return this;
    }

    @Override
    public void configure(ParameterTool parameterTool) {
        if (hasDefaultValue && !parameterTool.has(name)) {
            // skip checks for min and max when using default value
            value = defaultValue;
        } else {
            value = parameterTool.getLong(name);

            if (hasMinimumValue) {
                Util.checkParameter(
                        value >= minimumValue,
                        name + " must be greater than or equal to " + minimumValue);
            }

            if (hasMaximumValue) {
                Util.checkParameter(
                        value <= maximumValue,
                        name + " must be less than or equal to " + maximumValue);
            }
        }
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }
}
