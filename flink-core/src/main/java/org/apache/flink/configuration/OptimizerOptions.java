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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Configuration options for the optimizer. */
@PublicEvolving
public class OptimizerOptions {

    /**
     * The maximum number of line samples taken by the compiler for delimited inputs. The samples
     * are used to estimate the number of records. This value can be overridden for a specific input
     * with the input format’s parameters.
     */
    public static final ConfigOption<Integer> DELIMITED_FORMAT_MAX_LINE_SAMPLES =
            key("compiler.delimited-informat.max-line-samples")
                    .defaultValue(10)
                    .withDescription(
                            "The maximum number of line samples taken by the compiler for delimited inputs. The samples"
                                    + " are used to estimate the number of records. This value can be overridden for a specific input with the"
                                    + " input format’s parameters.");

    /**
     * The minimum number of line samples taken by the compiler for delimited inputs. The samples
     * are used to estimate the number of records. This value can be overridden for a specific input
     * with the input format’s parameters.
     */
    public static final ConfigOption<Integer> DELIMITED_FORMAT_MIN_LINE_SAMPLES =
            key("compiler.delimited-informat.min-line-samples")
                    .defaultValue(2)
                    .withDescription(
                            "The minimum number of line samples taken by the compiler for delimited inputs. The samples"
                                    + " are used to estimate the number of records. This value can be overridden for a specific input with the"
                                    + " input format’s parameters");

    /**
     * The maximal length of a line sample that the compiler takes for delimited inputs. If the
     * length of a single sample exceeds this value (possible because of misconfiguration of the
     * parser), the sampling aborts. This value can be overridden for a specific input with the
     * input format’s parameters.
     */
    public static final ConfigOption<Integer> DELIMITED_FORMAT_MAX_SAMPLE_LEN =
            key("compiler.delimited-informat.max-sample-len")
                    .defaultValue(2097152)
                    .withDescription(
                            "The maximal length of a line sample that the compiler takes for delimited inputs. If the"
                                    + " length of a single sample exceeds this value (possible because of misconfiguration of the parser),"
                                    + " the sampling aborts. This value can be overridden for a specific input with the input format’s"
                                    + " parameters.");
}
