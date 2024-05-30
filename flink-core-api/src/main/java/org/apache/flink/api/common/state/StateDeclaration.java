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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;

/** {@link StateDeclaration} represents a declaration of the specific state used. */
@Experimental
public interface StateDeclaration extends Serializable {

    /** Get the name of this state. */
    String getName();

    /**
     * Get the {@link RedistributionMode} of this state. More details see the doc of {@link
     * RedistributionMode}.
     */
    RedistributionMode getRedistributionMode();

    /**
     * {@link RedistributionMode} is used to indicate whether this state supports redistribution
     * between partitions and how to redistribute this state during rescaling.
     */
    @Experimental
    enum RedistributionMode {
        /**
         * Not supports redistribution.
         *
         * <p>For example : KeyedState is bind to a specific keyGroup, so it is can't support
         * redistribution between partitions.
         */
        NONE,

        /**
         * This state can be safely redistributed between different partitions, and the specific
         * redistribution strategy is determined by the state itself.
         *
         * <p>For example: ListState's redistribution algorithm is determined by {@link
         * ListStateDeclaration.RedistributionStrategy}.
         */
        REDISTRIBUTABLE,

        /**
         * States are guranteed to be identical in different partitions, thus redistribution is not
         * a problem.
         */
        IDENTICAL
    }
}
