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
import org.apache.flink.graph.drivers.parameter.DoubleParameter;
import org.apache.flink.types.CopyableValue;

import org.apache.commons.lang3.text.StrBuilder;
import org.apache.commons.lang3.text.WordUtils;

/** Driver for {@link org.apache.flink.graph.library.similarity.AdamicAdar}. */
public class AdamicAdar<K extends CopyableValue<K>, VV, EV> extends DriverBase<K, VV, EV> {

    private DoubleParameter minRatio =
            new DoubleParameter(this, "minimum_ratio")
                    .setDefaultValue(0.0)
                    .setMinimumValue(0.0, true);

    private DoubleParameter minScore =
            new DoubleParameter(this, "minimum_score")
                    .setDefaultValue(0.0)
                    .setMinimumValue(0.0, true);

    private BooleanParameter mirrorResults = new BooleanParameter(this, "mirror_results");

    @Override
    public String getShortDescription() {
        return "similarity score weighted by centerpoint degree";
    }

    @Override
    public String getLongDescription() {
        return WordUtils.wrap(
                new StrBuilder()
                        .appendln(
                                "Adamic-Adar measures the similarity between vertex neighborhoods and is "
                                        + "computed as the sum of the inverse logarithm of centerpoint degree over shared "
                                        + "neighbors.")
                        .appendNewLine()
                        .append(
                                "The algorithm result contains two vertex IDs and the similarity score.")
                        .toString(),
                80);
    }

    @Override
    public DataSet plan(Graph<K, VV, EV> graph) throws Exception {
        return graph.run(
                new org.apache.flink.graph.library.similarity.AdamicAdar<K, VV, EV>()
                        .setMinimumRatio(minRatio.getValue().floatValue())
                        .setMinimumScore(minScore.getValue().floatValue())
                        .setMirrorResults(mirrorResults.getValue())
                        .setParallelism(parallelism.getValue().intValue()));
    }
}
