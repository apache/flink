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

import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * An extension that is invoked before/after all/each tests, depending on whether it is wrapped in a
 * {@link EachCallbackWrapper} or {@link AllCallbackWrapper}.
 *
 * <p>{@code before} method will be called in {@code beforeEach} or {@code beforeAll}. {@code after}
 * will be called in {@code afterEach} or {@code afterAll}.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * public class Test{
 *      CustomExtension eachCustom = new CustomExtensionImpl1();
 *      CustomExtension allCustom = new CustomExtensionImpl2();
 *      @RegisterExtension
 *      static AllCallbackWrapper allWrapper = new AllCallbackWrapper(allCustom);
 *      @RegisterExtension
 *      EachCallbackWrapper eachWrapper = new EachCallbackWrapper(eachCustom);
 * }
 * }</pre>
 *
 * <p>A {@code CustomExtension} instance must not be wrapped in both AllCallbackWrapper and
 * EachCallbackWrapper for the same test class.
 */
public interface CustomExtension {
    default void before(ExtensionContext context) throws Exception {}

    default void after(ExtensionContext context) throws Exception {}
}
