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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.DistributionPattern;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Objects;

/** Bundled scalar parameters for POINTWISE-aware rescaling on a single gate or partition. */
public final class PointwiseRescaleParams implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final PointwiseRescaleParams EMPTY =
            new PointwiseRescaleParams(
                    DistributionPattern.ALL_TO_ALL, DistributionPattern.ALL_TO_ALL, 0, 0, 0, 0);

    private final DistributionPattern oldDistributionPattern;
    private final DistributionPattern newDistributionPattern;
    private final int oldUpParallelism;
    private final int oldDownParallelism;
    private final int newUpParallelism;
    private final int newDownParallelism;

    public PointwiseRescaleParams(
            DistributionPattern oldDistributionPattern,
            DistributionPattern newDistributionPattern,
            int oldUpParallelism,
            int oldDownParallelism,
            int newUpParallelism,
            int newDownParallelism) {
        this.oldDistributionPattern = oldDistributionPattern;
        this.newDistributionPattern = newDistributionPattern;
        this.oldUpParallelism = oldUpParallelism;
        this.oldDownParallelism = oldDownParallelism;
        this.newUpParallelism = newUpParallelism;
        this.newDownParallelism = newDownParallelism;
    }

    public DistributionPattern getOldDistributionPattern() {
        return oldDistributionPattern;
    }

    public DistributionPattern getNewDistributionPattern() {
        return newDistributionPattern;
    }

    public int getOldUpParallelism() {
        return oldUpParallelism;
    }

    public int getOldDownParallelism() {
        return oldDownParallelism;
    }

    public int getNewUpParallelism() {
        return newUpParallelism;
    }

    public int getNewDownParallelism() {
        return newDownParallelism;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PointwiseRescaleParams that = (PointwiseRescaleParams) o;
        return oldUpParallelism == that.oldUpParallelism
                && oldDownParallelism == that.oldDownParallelism
                && newUpParallelism == that.newUpParallelism
                && newDownParallelism == that.newDownParallelism
                && oldDistributionPattern == that.oldDistributionPattern
                && newDistributionPattern == that.newDistributionPattern;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                oldDistributionPattern,
                newDistributionPattern,
                oldUpParallelism,
                oldDownParallelism,
                newUpParallelism,
                newDownParallelism);
    }

    private Object readResolve() throws ObjectStreamException {
        if (oldUpParallelism == 0
                && oldDownParallelism == 0
                && newUpParallelism == 0
                && newDownParallelism == 0) {
            return EMPTY;
        }
        return this;
    }

    @Override
    public String toString() {
        return "PointwiseRescaleParams{"
                + "oldPattern="
                + oldDistributionPattern
                + ", newPattern="
                + newDistributionPattern
                + ", oldUp="
                + oldUpParallelism
                + ", oldDown="
                + oldDownParallelism
                + ", newUp="
                + newUpParallelism
                + ", newDown="
                + newDownParallelism
                + '}';
    }
}
