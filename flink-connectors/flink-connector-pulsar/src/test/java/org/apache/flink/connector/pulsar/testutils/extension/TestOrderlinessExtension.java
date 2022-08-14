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

package org.apache.flink.connector.pulsar.testutils.extension;

import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;

/** An extension for subclasses to specify {@link org.apache.pulsar.client.api.SubscriptionType}. */
public class TestOrderlinessExtension implements BeforeAllCallback {

    public static final ExtensionContext.Namespace PULSAR_TEST_RESOURCE_NAMESPACE =
            ExtensionContext.Namespace.create("pulsarTestResourceNamespace");
    public static final String PULSAR_SOURCE_READER_SUBSCRIPTION_TYPE_STORE_KEY =
            "pulsarSourceReaderSubscriptionTypeStoreKey";

    private SubscriptionType subscriptionType;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        final List<SubscriptionType> subscriptionTypes =
                AnnotationSupport.findAnnotatedFieldValues(
                        context.getRequiredTestInstance(), SubType.class, SubscriptionType.class);
        checkExactlyOneAnnotatedField(subscriptionTypes, SubType.class);
        subscriptionType = subscriptionTypes.get(0);
        context.getStore(PULSAR_TEST_RESOURCE_NAMESPACE)
                .put(PULSAR_SOURCE_READER_SUBSCRIPTION_TYPE_STORE_KEY, subscriptionType);
    }

    private void checkExactlyOneAnnotatedField(
            Collection<?> fields, Class<? extends Annotation> annotation) {
        if (fields.size() > 1) {
            throw new IllegalStateException(
                    String.format(
                            "Multiple fields are annotated with '@%s'",
                            annotation.getSimpleName()));
        }
        if (fields.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "No fields are annotated with '@%s'", annotation.getSimpleName()));
        }
    }
}
