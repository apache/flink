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

package org.apache.flink.table.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;

/**
 * Factory for {@link ExpressionParser}.
 *
 * @deprecated The Java String Expression DSL is deprecated.
 */
@Internal
@Deprecated
public interface ExpressionParserFactory extends Factory {

    /** {@link #factoryIdentifier()} for the default {@link ExpressionParserFactory}. */
    String DEFAULT_IDENTIFIER = "default";

    ExpressionParser create();

    static ExpressionParserFactory getDefault() {
        return FactoryUtil.discoverFactory(
                Thread.currentThread().getContextClassLoader(),
                ExpressionParserFactory.class,
                DEFAULT_IDENTIFIER);
    }
}
