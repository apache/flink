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

package org.apache.flink.util;

import org.apache.flink.types.StringValue;
import org.apache.flink.util.StringValueUtils.WhitespaceTokenizer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link StringValueUtils}. */
class StringValueUtilsTest {

    @Test
    void testToLowerCaseConverting() {
        StringValue testString = new StringValue("TEST");
        StringValueUtils.toLowerCase(testString);
        assertThat((Object) testString).isEqualTo(new StringValue("test"));
    }

    @Test
    void testReplaceNonWordChars() {
        StringValue testString = new StringValue("TEST123_@");
        StringValueUtils.replaceNonWordChars(testString, '!');
        assertThat((Object) testString).isEqualTo(new StringValue("TEST123_!"));
    }

    @Test
    void testTokenizerOnStringWithoutNexToken() {
        StringValue testString = new StringValue("test");
        StringValueUtils.WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setStringToTokenize(testString);
        // first token
        tokenizer.next(testString);
        // next token is not exist
        assertThat(tokenizer.next(testString)).isFalse();
    }

    @Test
    void testTokenizerOnStringWithNexToken() {
        StringValue testString = new StringValue("test test");
        StringValueUtils.WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setStringToTokenize(testString);
        assertThat(tokenizer.next(testString)).isTrue();
    }

    @Test
    void testTokenizerOnStringOnlyWithDelimiter() {
        StringValue testString = new StringValue("    ");
        StringValueUtils.WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setStringToTokenize(testString);
        assertThat(tokenizer.next(testString)).isFalse();
    }
}
