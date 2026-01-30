/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.model.triton;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link TritonModelProviderFactory}. */
public class TritonModelProviderFactoryTest {

    @Test
    public void testFactoryIdentifier() {
        TritonModelProviderFactory factory = new TritonModelProviderFactory();
        assertEquals("triton", factory.factoryIdentifier());
    }

    @Test
    public void testRequiredOptions() {
        TritonModelProviderFactory factory = new TritonModelProviderFactory();
        assertEquals(2, factory.requiredOptions().size());
        assertTrue(factory.requiredOptions().contains(AbstractTritonModelFunction.ENDPOINT));
        assertTrue(factory.requiredOptions().contains(AbstractTritonModelFunction.MODEL_NAME));
    }

    @Test
    public void testOptionalOptions() {
        TritonModelProviderFactory factory = new TritonModelProviderFactory();
        assertEquals(12, factory.optionalOptions().size());
        assertTrue(factory.optionalOptions().contains(AbstractTritonModelFunction.MODEL_VERSION));
        assertTrue(factory.optionalOptions().contains(AbstractTritonModelFunction.TIMEOUT));
        assertTrue(factory.optionalOptions().contains(AbstractTritonModelFunction.BATCH_SIZE));
        assertTrue(
                factory.optionalOptions().contains(AbstractTritonModelFunction.FLATTEN_BATCH_DIM));
        assertTrue(factory.optionalOptions().contains(AbstractTritonModelFunction.PRIORITY));
        assertTrue(factory.optionalOptions().contains(AbstractTritonModelFunction.SEQUENCE_ID));
        assertTrue(factory.optionalOptions().contains(AbstractTritonModelFunction.SEQUENCE_START));
        assertTrue(factory.optionalOptions().contains(AbstractTritonModelFunction.SEQUENCE_END));
        assertTrue(factory.optionalOptions().contains(AbstractTritonModelFunction.COMPRESSION));
        assertTrue(factory.optionalOptions().contains(AbstractTritonModelFunction.AUTH_TOKEN));
        assertTrue(factory.optionalOptions().contains(AbstractTritonModelFunction.CUSTOM_HEADERS));
    }
}
