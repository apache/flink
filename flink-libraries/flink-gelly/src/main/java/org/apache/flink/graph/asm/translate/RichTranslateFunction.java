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

package org.apache.flink.graph.asm.translate;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.RichFunction;

/**
 * Rich variant of the {@link TranslateFunction}. As a {@link RichFunction}, it gives access to the
 * {@link org.apache.flink.api.common.functions.RuntimeContext} and provides setup and teardown
 * methods: {@link RichFunction#open(org.apache.flink.configuration.Configuration)} and {@link
 * RichFunction#close()}.
 *
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the returned elements.
 */
public abstract class RichTranslateFunction<IN, OUT> extends AbstractRichFunction
        implements TranslateFunction<IN, OUT> {

    private static final long serialVersionUID = 1L;

    @Override
    public abstract OUT translate(IN value, OUT reuse) throws Exception;
}
