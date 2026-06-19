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

package org.apache.flink.configuration;

import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StructuredOptionsSplitter#escapeWithSingleQuote}. */
@ExtendWith(ParameterizedTestExtension.class)
class StructuredOptionsSplitterEscapeTest {

    @Parameters(name = "testSpec = {0}")
    private static Collection<TestSpec> getEncodeSpecs() {
        return Arrays.asList(
                TestSpec.encode("A,B,C,D", ";").expect("A,B,C,D"),
                TestSpec.encode("A;BCD", ";").expect("'A;BCD'"),
                TestSpec.encode("A'B'C'D", ";").expect("'A''B''C''D'"),
                TestSpec.encode("AB\"C\"D", ";").expect("'AB\"C\"D'"),
                TestSpec.encode("AB'\"D:B", ";").expect("'AB''\"D:B'"),
                TestSpec.encode("A,B,C,D", ",").expect("'A,B,C,D'"),
                TestSpec.encode("A;BCD", ",").expect("A;BCD"),
                TestSpec.encode("AB\"C\"D", ",").expect("'AB\"C\"D'"),
                TestSpec.encode("AB'\"D:B", ",").expect("'AB''\"D:B'"),
                TestSpec.encode("A;B;C;D", ",", ":").expect("A;B;C;D"),
                TestSpec.encode("A;B;C:D", ",", ":").expect("'A;B;C:D'"));
    }

    @Parameter private TestSpec testSpec;

    @TestTemplate
    void testEscapeWithSingleQuote() {
        String encoded =
                StructuredOptionsSplitter.escapeWithSingleQuote(
                        testSpec.getString(), testSpec.getEscapeChars());
        assertThat(encoded).isEqualTo(testSpec.getEncodedString());
    }

    private static class TestSpec {
        private final String string;
        private final String[] escapeChars;
        private String encodedString;

        private TestSpec(String string, String... escapeChars) {
            this.string = string;
            this.escapeChars = escapeChars;
        }

        public static TestSpec encode(String string, String... escapeChars) {
            return new TestSpec(string, escapeChars);
        }

        public TestSpec expect(String string) {
            this.encodedString = string;
            return this;
        }

        public String getString() {
            return string;
        }

        public String getEncodedString() {
            return encodedString;
        }

        public String[] getEscapeChars() {
            return escapeChars;
        }
    }
}
