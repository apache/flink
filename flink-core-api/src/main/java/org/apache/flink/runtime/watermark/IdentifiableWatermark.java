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

package org.apache.flink.runtime.watermark;

import java.io.Serializable;

/**
 * The {@link IdentifiableWatermark} interface represents a watermark that can be identified by a
 * unique identifier.
 *
 * <p>Implementing classes should provide a concrete implementation of the {@code getIdentifier}
 * method to return a unique identifier for the watermark.
 *
 * @see Serializable
 */
public interface IdentifiableWatermark extends Serializable {

    /**
     * Returns the unique identifier for this watermark.
     *
     * @return a {@code String} representing the unique identifier of the watermark
     */
    String getIdentifier();
}
