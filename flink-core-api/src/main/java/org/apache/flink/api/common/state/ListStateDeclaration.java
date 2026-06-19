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
import org.apache.flink.api.common.typeinfo.TypeDescriptor;

/** This represents a declaration of the list state. */
@Experimental
public interface ListStateDeclaration<T> extends StateDeclaration {
    /**
     * Get the {@link RedistributionStrategy} of this list state.
     *
     * @return the redistribution strategy of this list state.
     */
    RedistributionStrategy getRedistributionStrategy();

    /**
     * {@link RedistributionStrategy} is used to guide the assignment of states during rescaling.
     */
    @Experimental
    enum RedistributionStrategy {
        /**
         * The whole state is logically a concatenation of all lists. On restore/redistribution, the
         * list is evenly divided into as many sub-lists as there are parallel operators. Each
         * operator gets a sub-list, which can be empty, or contain one or more elements.
         */
        SPLIT,
        /**
         * The whole state is logically a concatenation of all lists. On restore/redistribution,
         * each operator gets the complete list of state elements.
         */
        UNION
    }

    /** Get type descriptor of this list state's element. */
    TypeDescriptor<T> getTypeDescriptor();
}
