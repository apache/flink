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

package org.apache.flink.api.common.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;

import java.io.IOException;

/** Generic base class for all Rich inputs that are not based on files. */
@Public
public abstract class GenericInputFormat<OT> extends RichInputFormat<OT, GenericInputSplit> {

    private static final long serialVersionUID = 1L;

    /** The partition of this split. */
    protected int partitionNumber;

    // --------------------------------------------------------------------------------------------

    @Override
    public void configure(Configuration parameters) {
        //	nothing by default
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        // no statistics available, by default.
        return cachedStatistics;
    }

    @Override
    public GenericInputSplit[] createInputSplits(int numSplits) throws IOException {
        if (numSplits < 1) {
            throw new IllegalArgumentException("Number of input splits has to be at least 1.");
        }

        numSplits = (this instanceof NonParallelInput) ? 1 : numSplits;
        GenericInputSplit[] splits = new GenericInputSplit[numSplits];
        for (int i = 0; i < splits.length; i++) {
            splits[i] = new GenericInputSplit(i, numSplits);
        }
        return splits;
    }

    @Override
    public DefaultInputSplitAssigner getInputSplitAssigner(GenericInputSplit[] splits) {
        return new DefaultInputSplitAssigner(splits);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void open(GenericInputSplit split) throws IOException {
        this.partitionNumber = split.getSplitNumber();
    }

    @Override
    public void close() throws IOException {}
}
