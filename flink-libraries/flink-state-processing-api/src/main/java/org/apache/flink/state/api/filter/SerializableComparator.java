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

package org.apache.flink.state.api.filter;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;
import java.util.Comparator;

/**
 * A {@link Comparator} that is also {@link Serializable}.
 *
 * <p>Required when passing a custom comparator to {@link SavepointKeyFilter#range} because the
 * filter is serialized and shipped with the job. Lambdas and method references assigned to this
 * type are automatically serializable.
 *
 * @param <K> The type of keys being compared.
 */
@Experimental
@FunctionalInterface
public interface SerializableComparator<K> extends Comparator<K>, Serializable {
    long serialVersionUID = 1L;
}
