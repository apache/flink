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

package org.apache.flink.connector.base.sink.writer.strategy;

import org.apache.flink.annotation.PublicEvolving;

/** ScalingStrategy provides an interface to control scale up / down behaviour. */
@PublicEvolving
public interface ScalingStrategy<T> {

    /**
     * Returns the scaled up value.
     *
     * @param currentValue currentValue
     */
    T scaleUp(T currentValue);

    /**
     * Returns the scaled down value.
     *
     * @param currentValue currentValue
     */
    T scaleDown(T currentValue);
}
