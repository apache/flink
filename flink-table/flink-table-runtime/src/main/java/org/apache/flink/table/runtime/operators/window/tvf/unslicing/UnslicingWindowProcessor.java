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

package org.apache.flink.table.runtime.operators.window.tvf.unslicing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAggOperator;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowProcessor;

/**
 * The {@link UnslicingWindowProcessor} is an optimized processing for unaligned windows.
 *
 * <p>A {@link UnslicingWindowProcessor} usually leverages the {@link UnsliceAssigner} to assign
 * slices and calculate based on the window.
 *
 * <p>Note: Currently, the {@link UnslicingWindowProcessor} only support session time window.
 *
 * <p>See more details in {@link WindowAggOperator}.
 */
@Internal
public interface UnslicingWindowProcessor<W> extends WindowProcessor<W> {}
