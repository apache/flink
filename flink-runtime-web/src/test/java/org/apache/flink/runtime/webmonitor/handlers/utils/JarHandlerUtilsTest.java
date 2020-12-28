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

package org.apache.flink.runtime.webmonitor.handlers.utils;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/** Tests for {@link JarHandlerUtils}. */
public class JarHandlerUtilsTest extends TestLogger {

    @Test
    public void testTokenizeNonQuoted() {
        final List<String> arguments = JarHandlerUtils.tokenizeArguments("--foo bar");
        assertThat(arguments.get(0), equalTo("--foo"));
        assertThat(arguments.get(1), equalTo("bar"));
    }

    @Test
    public void testTokenizeSingleQuoted() {
        final List<String> arguments = JarHandlerUtils.tokenizeArguments("--foo 'bar baz '");
        assertThat(arguments.get(0), equalTo("--foo"));
        assertThat(arguments.get(1), equalTo("bar baz "));
    }

    @Test
    public void testTokenizeDoubleQuoted() {
        final List<String> arguments = JarHandlerUtils.tokenizeArguments("--name \"K. Bote \"");
        assertThat(arguments.get(0), equalTo("--name"));
        assertThat(arguments.get(1), equalTo("K. Bote "));
    }
}
