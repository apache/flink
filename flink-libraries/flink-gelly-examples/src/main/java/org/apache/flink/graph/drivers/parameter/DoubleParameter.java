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

/** A {@link Parameter} storing a {@link Double}. */
public class DoubleParameter extends SimpleParameter<Double> {

    private boolean hasMinimumValue = false;
    private boolean minimumValueInclusive;
    private double minimumValue;

    private boolean hasMaximumValue = false;
    private boolean maximumValueInclusive;
    private double maximumValue;

    /**
     * Set the parameter name and add this parameter to the list of parameters stored by owner.
     *
     * @param owner the {@link Parameterized} using this {@link Parameter}
     * @param name the parameter name
     */
    public DoubleParameter(ParameterizedBase owner, String name) {
        super(owner, name);
    }

    /**
     * Set the default value.
     *
     * @param defaultValue the default value
     * @return this
     */
    public DoubleParameter setDefaultValue(double defaultValue) {
        super.setDefaultValue(defaultValue);

        if (hasMinimumValue) {
            if (minimumValueInclusive) {
                Util.checkParameter(
                        defaultValue >= minimumValue,
                        "Default value ("
                                + defaultValue
                                + ") must be greater than or equal to minimum ("
                                + minimumValue
                                + ")");
            } else {
                Util.checkParameter(
                        defaultValue > minimumValue,
                        "Default value ("
                                + defaultValue
                                + ") must be greater than minimum ("
                                + minimumValue
                                + ")");
            }
        }

        if (hasMaximumValue) {
            if (maximumValueInclusive) {
                Util.checkParameter(
                        defaultValue <= maximumValue,
                        "Default value ("
                                + defaultValue
                                + ") must be less than or equal to maximum ("
                                + maximumValue
                                + ")");
            } else {
                Util.checkParameter(
                        defaultValue < maximumValue,
                        "Default value ("
                                + defaultValue
                                + ") must be less than maximum ("
                                + maximumValue
                                + ")");
            }
        }

        return this;
    }

    /**
     * Set the minimum value. The minimum value is an acceptable value if and only if inclusive is
     * set to true.
     *
     * @param minimumValue the minimum value
     * @param inclusive whether the minimum value is a valid value
     * @return this
     */
    public DoubleParameter setMinimumValue(double minimumValue, boolean inclusive) {
        if (hasDefaultValue) {
            if (inclusive) {
                Util.checkParameter(
                        minimumValue <= defaultValue,
                        "Minimum value ("
                                + minimumValue
                                + ") must be less than or equal to default ("
                                + defaultValue
                                + ")");
            } else {
                Util.checkParameter(
                        minimumValue < defaultValue,
                        "Minimum value ("
                                + minimumValue
                                + ") must be less than default ("
                                + defaultValue
                                + ")");
            }
        } else if (hasMaximumValue) {
            if (inclusive && maximumValueInclusive) {
                Util.checkParameter(
                        minimumValue <= maximumValue,
                        "Minimum value ("
                                + minimumValue
                                + ") must be less than or equal to maximum ("
                                + maximumValue
                                + ")");
            } else {
                Util.checkParameter(
                        minimumValue < maximumValue,
                        "Minimum value ("
                                + minimumValue
                                + ") must be less than maximum ("
                                + maximumValue
                                + ")");
            }
        }

        this.hasMinimumValue = true;
        this.minimumValue = minimumValue;
        this.minimumValueInclusive = inclusive;

        return this;
    }

    /**
     * Set the maximum value. The maximum value is an acceptable value if and only if inclusive is
     * set to true.
     *
     * @param maximumValue the maximum value
     * @param inclusive whether the maximum value is a valid value
     * @return this
     */
    public DoubleParameter setMaximumValue(double maximumValue, boolean inclusive) {
        if (hasDefaultValue) {
            if (inclusive) {
                Util.checkParameter(
                        maximumValue >= defaultValue,
                        "Maximum value ("
                                + maximumValue
                                + ") must be greater than or equal to default ("
                                + defaultValue
                                + ")");
            } else {
                Util.checkParameter(
                        maximumValue > defaultValue,
                        "Maximum value ("
                                + maximumValue
                                + ") must be greater than default ("
                                + defaultValue
                                + ")");
            }
        } else if (hasMinimumValue) {
            if (inclusive && minimumValueInclusive) {
                Util.checkParameter(
                        maximumValue >= minimumValue,
                        "Maximum value ("
                                + maximumValue
                                + ") must be greater than or equal to minimum ("
                                + minimumValue
                                + ")");
            } else {
                Util.checkParameter(
                        maximumValue > minimumValue,
                        "Maximum value ("
                                + maximumValue
                                + ") must be greater than minimum ("
                                + minimumValue
                                + ")");
            }
        }

        this.hasMaximumValue = true;
        this.maximumValue = maximumValue;
        this.maximumValueInclusive = inclusive;

        return this;
    }

    @Override
    public void configure(ParameterTool parameterTool) {
        value =
                hasDefaultValue
                        ? parameterTool.getDouble(name, defaultValue)
                        : parameterTool.getDouble(name);

        if (hasMinimumValue) {
            if (minimumValueInclusive) {
                Util.checkParameter(
                        value >= minimumValue,
                        name + " must be greater than or equal to " + minimumValue);
            } else {
                Util.checkParameter(
                        value > minimumValue, name + " must be greater than " + minimumValue);
            }
        }

        if (hasMaximumValue) {
            if (maximumValueInclusive) {
                Util.checkParameter(
                        value <= maximumValue,
                        name + " must be less than or equal to " + maximumValue);
            } else {
                Util.checkParameter(
                        value < maximumValue, name + " must be less than " + maximumValue);
            }
        }
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }
}
