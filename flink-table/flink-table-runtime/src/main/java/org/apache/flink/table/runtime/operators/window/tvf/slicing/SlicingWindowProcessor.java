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

package org.apache.flink.table.runtime.operators.window.tvf.slicing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.runtime.operators.aggregate.window.processors.SliceSharedWindowAggProcessor;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAggOperator;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowProcessor;

/**
 * The {@link SlicingWindowProcessor} is an optimized processing for aligned windows which can apply
 * the slicing optimization. The core idea of slicing optimization is to divide all elements from a
 * data stream into a finite number of non-overlapping chunks (a.k.a. slices).
 *
 * <h3>Concept of Slice</h3>
 *
 * <p>Dividing a window of aligned windows into a finite number of non-overlapping chunks, where the
 * chunks are slices. It has the following properties:
 *
 * <ul>
 *   <li>An element must only belong to a single slice.
 *   <li>Slices are non-overlapping, i.e. S_i and S_j should not have any shared elements if i != j.
 *   <li>A window is consist of a finite number of slices.
 * </ul>
 *
 * <p>The {@link SlicingWindowProcessor} have different implementation for aggregate and topk or
 * others.
 *
 * <p>The {@link SlicingWindowProcessor} usually leverages the {@link SliceAssigner} to assign
 * slices and calculate based on the slices. See {@link SliceSharedWindowAggProcessor} as an
 * example.
 *
 * <p>Note: since {@link SlicingWindowProcessor} leverages slicing optimization for aligned windows,
 * therefore, it doesn't support unaligned windows, e.g. session window.
 *
 * <p>See more details in {@link WindowAggOperator}.
 */
@Internal
public interface SlicingWindowProcessor<W> extends WindowProcessor<W> {}
