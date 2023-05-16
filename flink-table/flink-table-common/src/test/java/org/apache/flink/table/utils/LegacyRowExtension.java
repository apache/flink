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

package org.apache.flink.table.utils;

import org.apache.flink.core.testutils.CustomExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowUtils;

import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Enables the old behavior of {@link Row#toString()} for legacy tests.
 *
 * <p>It ignores the changes applied in FLINK-16998 and FLINK-19981.
 */
public class LegacyRowExtension implements CustomExtension {
    @Override
    public void before(ExtensionContext context) throws Exception {
        RowUtils.USE_LEGACY_TO_STRING = true;
    }

    @Override
    public void after(ExtensionContext context) throws Exception {
        RowUtils.USE_LEGACY_TO_STRING = false;
    }
}
