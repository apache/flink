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

package org.apache.flink.graph.drivers;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.drivers.parameter.BooleanParameter;
import org.apache.flink.graph.drivers.parameter.LongParameter;
import org.apache.flink.types.CopyableValue;

import org.apache.commons.lang3.text.StrBuilder;
import org.apache.commons.lang3.text.WordUtils;

/** Driver for {@link org.apache.flink.graph.library.similarity.JaccardIndex}. */
public class JaccardIndex<K extends CopyableValue<K>, VV, EV> extends DriverBase<K, VV, EV> {

    private LongParameter minNumerator =
            new LongParameter(this, "minimum_numerator").setDefaultValue(0).setMinimumValue(0);

    private LongParameter minDenominator =
            new LongParameter(this, "minimum_denominator").setDefaultValue(1).setMinimumValue(1);

    private LongParameter maxNumerator =
            new LongParameter(this, "maximum_numerator").setDefaultValue(1).setMinimumValue(0);

    private LongParameter maxDenominator =
            new LongParameter(this, "maximum_denominator").setDefaultValue(1).setMinimumValue(1);

    private BooleanParameter mirrorResults = new BooleanParameter(this, "mirror_results");

    @Override
    public String getShortDescription() {
        return "similarity score as fraction of common neighbors";
    }

    @Override
    public String getLongDescription() {
        return WordUtils.wrap(
                new StrBuilder()
                        .appendln(
                                "Jaccard Index measures the similarity between vertex neighborhoods and "
                                        + "is computed as the number of shared neighbors divided by the number of "
                                        + "distinct neighbors. Scores range from 0.0 (no shared neighbors) to 1.0 (all "
                                        + "neighbors are shared).")
                        .appendNewLine()
                        .append(
                                "The result contains two vertex IDs, the number of shared neighbors, and "
                                        + "the number of distinct neighbors.")
                        .toString(),
                80);
    }

    @Override
    public DataSet plan(Graph<K, VV, EV> graph) throws Exception {
        return graph.run(
                new org.apache.flink.graph.library.similarity.JaccardIndex<K, VV, EV>()
                        .setMinimumScore(
                                minNumerator.getValue().intValue(),
                                minDenominator.getValue().intValue())
                        .setMaximumScore(
                                maxNumerator.getValue().intValue(),
                                maxDenominator.getValue().intValue())
                        .setMirrorResults(mirrorResults.getValue())
                        .setParallelism(parallelism.getValue().intValue()));
    }
}
