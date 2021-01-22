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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.InjectableValues;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.DeserializerFactory;

/** Custom JSON {@link DeserializationContext} which wraps a {@link SerdeContext}. */
public class FlinkDeserializationContext extends DefaultDeserializationContext {
    private static final long serialVersionUID = 1L;
    private final SerdeContext serdeCtx;

    public FlinkDeserializationContext(DefaultDeserializationContext src, SerdeContext serdeCtx) {
        super(src);
        this.serdeCtx = serdeCtx;
    }

    protected FlinkDeserializationContext(
            FlinkDeserializationContext src,
            DeserializationConfig config,
            JsonParser jp,
            InjectableValues values) {
        super(src, config, jp, values);
        this.serdeCtx = src.serdeCtx;
    }

    protected FlinkDeserializationContext(
            FlinkDeserializationContext src, DeserializerFactory factory) {
        super(src, factory);
        this.serdeCtx = src.serdeCtx;
    }

    @Override
    public DefaultDeserializationContext with(DeserializerFactory factory) {
        return new FlinkDeserializationContext(this, factory);
    }

    @Override
    public DefaultDeserializationContext createInstance(
            DeserializationConfig config, JsonParser p, InjectableValues values) {
        return new FlinkDeserializationContext(this, config, p, values);
    }

    public SerdeContext getSerdeContext() {
        return serdeCtx;
    }
}
