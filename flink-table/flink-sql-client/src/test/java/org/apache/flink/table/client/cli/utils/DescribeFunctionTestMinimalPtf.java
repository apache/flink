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

package org.apache.flink.table.client.cli.utils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

/**
 * Minimal Process Table Function used by {@code function.q} to exercise the {@code false} side of
 * the per-PTF capability flags in {@code DESCRIBE FUNCTION EXTENDED}: no {@link
 * org.apache.flink.table.annotation.StateHint StateHint} (no {@code state:} row), does not
 * implement {@link org.apache.flink.table.functions.ChangelogFunction ChangelogFunction} ({@code is
 * changelog function = false}), and declares no {@code onTimer} ({@code uses timers = false}).
 */
public class DescribeFunctionTestMinimalPtf extends ProcessTableFunction<Long> {

    public void eval(@ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) Row input) {}
}
