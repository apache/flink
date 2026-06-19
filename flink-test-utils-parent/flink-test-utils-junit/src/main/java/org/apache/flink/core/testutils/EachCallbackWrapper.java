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

package org.apache.flink.core.testutils;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/** An extension wrap logic for {@link BeforeEachCallback} and {@link AfterEachCallback}. */
public class EachCallbackWrapper<C extends CustomExtension>
        implements BeforeEachCallback, AfterEachCallback {
    private final C customExtension;

    public EachCallbackWrapper(C customExtension) {
        this.customExtension = customExtension;
    }

    public C getCustomExtension() {
        return customExtension;
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        customExtension.after(context);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        customExtension.before(context);
    }
}
