/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link Reference}. */
@ExtendWith({TestLoggerExtension.class})
public class ReferenceTest {

    @Test
    public void testOwnedReferenceIsOwned() {
        final Reference<String> value = Reference.owned("foobar");

        assertThat(value.isOwned()).isTrue();
    }

    @Test
    public void testBorrowedReferenceIsNotOwned() {
        final Reference<String> value = Reference.borrowed("foobar");

        assertThat(value.isOwned()).isFalse();
    }

    @Test
    public void testOwnedReferenceReturnsSomeOwned() {
        final String value = "foobar";
        final Reference<String> owned = Reference.owned(value);

        assertThat(owned.owned()).hasValue(value);
    }

    @Test
    public void testBorrowedReferenceReturnsEmptyOwned() {
        Reference<String> value = Reference.borrowed("foobar");

        assertThat(value.owned()).isEmpty();
    }
}
