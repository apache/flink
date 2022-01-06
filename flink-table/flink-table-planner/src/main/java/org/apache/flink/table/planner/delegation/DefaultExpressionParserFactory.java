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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.delegation.ExpressionParser;
import org.apache.flink.table.delegation.ExpressionParserFactory;
import org.apache.flink.table.expressions.ExpressionParserImpl;

import java.util.Collections;
import java.util.Set;

/** Default factory for {@link ExpressionParser}. */
@Internal
public final class DefaultExpressionParserFactory implements ExpressionParserFactory {

    @Override
    public ExpressionParser create() {
        return new ExpressionParserImpl();
    }

    @Override
    public String factoryIdentifier() {
        return ExpressionParserFactory.DEFAULT_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
