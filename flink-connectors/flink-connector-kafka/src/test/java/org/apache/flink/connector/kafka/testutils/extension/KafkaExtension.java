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

package org.apache.flink.connector.kafka.testutils.extension;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.testutils.annotations.Kafka;
import org.apache.flink.connector.kafka.testutils.annotations.KafkaKit;
import org.apache.flink.connector.kafka.testutils.annotations.Topic;
import org.apache.flink.connector.kafka.testutils.cluster.KafkaContainers;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.AnnotationUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * JUnit 5 extension for setup Kafka cluster and related resources for tests.
 *
 * <p>This extension will start {@link KafkaContainers} before all tests, and create topics
 * specified by annotation {@link Topic}.
 *
 * <p>Please use {@link KafkaClientKit} for interacting with the Kafka cluster in this extension.
 *
 * <p>For most cases using the shortcut annotation {@link Kafka} is sufficient and more convenient,
 * but if you need to customize Kafka containers or interacting with other extensions, you may want
 * to construct and register this extension manually.
 */
public class KafkaExtension
        implements BeforeAllCallback,
                AfterAllCallback,
                BeforeEachCallback,
                AfterEachCallback,
                ParameterResolver {

    /** Default timeout for async operations. */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(10);

    /** Kafka containers. */
    private final KafkaContainers kafkaContainers;

    /** Admin client connected to the Kafka cluster. */
    private AdminClient adminClient;

    private final Set<Tuple2<String, Lifecycle>> managedTopics = new HashSet<>();

    /** Constructs extension with default configurations. */
    public KafkaExtension() {
        this(KafkaContainers.builder().build());
    }

    /** Reusable Kafka context. */
    private KafkaClientKit kafkaClientKit;

    /** Constructs extension with given Kafka container. */
    public KafkaExtension(KafkaContainers kafkaContainers) {
        this.kafkaContainers = kafkaContainers;
    }

    /** Get the bootstrap servers of the Kafka cluster. */
    public String getBootstrapServers() {
        return kafkaContainers.getBootstrapServers();
    }

    /** Get Kafka containers. */
    public KafkaContainers getKafkaContainers() {
        return kafkaContainers;
    }

    /** Create {@link KafkaClientKit} for interacting with the Kafka cluster. */
    public KafkaClientKit createKafkaClientKit() {
        if (kafkaClientKit != null) {
            return kafkaClientKit;
        }
        checkState(kafkaContainers.isRunning(), "Kafka container is not running");
        checkNotNull(adminClient, "AdminClient should not be null");
        kafkaClientKit = new KafkaClientKit(kafkaContainers.getBootstrapServers(), adminClient);
        return kafkaClientKit;
    }

    // ----------------------------- JUnit 5 related --------------------------

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        startKafkaCluster();
        createTopics(context, Lifecycle.PER_CLASS);
        injectKafkaClientFields(context, Lifecycle.PER_CLASS);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        deleteTopics(Lifecycle.PER_CLASS);
        stopKafkaCluster();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        // Create topics annotated on the test method
        createTopics(context, Lifecycle.PER_METHOD);
        injectKafkaClientFields(context, Lifecycle.PER_METHOD);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        deleteTopics(Lifecycle.PER_METHOD);
    }

    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return isKafkaKitParameter(parameterContext);
    }

    @Override
    public Object resolveParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return createKafkaClientKit();
    }

    // ------------------------------- Helper functions ---------------------------

    public void startKafkaCluster() {
        // Start Kafka container
        kafkaContainers.start();
        // Create admin client
        Properties props = new Properties();
        props.setProperty(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainers.getBootstrapServers());
        adminClient = AdminClient.create(props);
    }

    public void stopKafkaCluster() {
        if (adminClient != null) {
            adminClient.close(DEFAULT_TIMEOUT);
        }
        kafkaContainers.stop();
    }

    private void injectKafkaClientFields(ExtensionContext context, Lifecycle lifecycle)
            throws IllegalAccessException {
        List<Field> kafkaClientFields =
                getAnnotatedFieldByLifecycle(context, KafkaKit.class, lifecycle);
        for (Field kafkaClientField : kafkaClientFields) {
            checkAnnotatedFieldType(kafkaClientField, KafkaKit.class, KafkaClientKit.class);
            kafkaClientField.setAccessible(true);
            kafkaClientField.set(context.getTestInstance().orElse(null), createKafkaClientKit());
        }
    }

    private void createTopics(ExtensionContext extensionContext, Lifecycle lifecycle)
            throws Exception {
        for (Field topicNameField :
                getAnnotatedFieldByLifecycle(extensionContext, Topic.class, lifecycle)) {
            // Check the annotated field
            Topic topic = topicNameField.getAnnotation(Topic.class);
            Topic.Validator.validate(topic, topicNameField);
            // Get name of the topic
            topicNameField.setAccessible(true);
            String topicName =
                    (String) topicNameField.get(extensionContext.getTestInstance().orElse(null));
            String finalTopicName = topicName;
            // Create a random topic name if not specified
            if (topicName == null || topicName.isEmpty()) {
                finalTopicName = String.valueOf(UUID.randomUUID());
            }
            // Randomize the topic name
            if (topic.randomizeName()) {
                finalTopicName = topicName + "-" + UUID.randomUUID();
            }
            // Create the topic
            adminClient
                    .createTopics(
                            Collections.singletonList(
                                    new NewTopic(
                                            finalTopicName,
                                            topic.numPartitions(),
                                            topic.replicationFactor())))
                    .all()
                    .get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            managedTopics.add(Tuple2.of(finalTopicName, lifecycle));
            // Set the final topic name back to the field
            if (!finalTopicName.equals(topicName)) {
                topicNameField.setAccessible(true);
                topicNameField.set(extensionContext.getTestInstance().orElse(null), finalTopicName);
            }
        }
    }

    private void deleteTopics(Lifecycle lifecycle) throws Exception {
        Set<Tuple2<String, Lifecycle>> topicsToDelete = new HashSet<>();
        for (Tuple2<String, Lifecycle> topic : managedTopics) {
            if (topic.f1.equals(lifecycle)) {
                topicsToDelete.add(topic);
            }
        }
        adminClient
                .deleteTopics(
                        topicsToDelete.stream().map(topic -> topic.f0).collect(Collectors.toSet()))
                .all()
                .get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        managedTopics.removeAll(topicsToDelete);
    }

    private enum Lifecycle {
        PER_CLASS,
        PER_METHOD
    }

    private List<Field> getAnnotatedFieldByLifecycle(
            ExtensionContext context,
            Class<? extends Annotation> annotationClass,
            Lifecycle lifecycle) {
        Predicate<Field> fieldPredicate;
        switch (lifecycle) {
            case PER_CLASS:
                fieldPredicate = field -> Modifier.isStatic(field.getModifiers());
                break;
            case PER_METHOD:
                fieldPredicate = field -> !Modifier.isStatic(field.getModifiers());
                break;
            default:
                throw new IllegalArgumentException(
                        "Topic lifecycle should be either PER_CLASS or PER_INSTANCE");
        }
        return AnnotationUtils.findAnnotatedFields(
                context.getRequiredTestClass(), annotationClass, fieldPredicate);
    }

    private void checkAnnotatedTopic(Field topicNameField, Topic topic) {
        // @Topic should be used on string fields
        checkAnnotatedFieldType(topicNameField, Topic.class, String.class);
        // Randomizing topic name is not applicable for final fields
        if (topic.randomizeName()) {
            checkState(
                    !Modifier.isFinal(topicNameField.getModifiers()),
                    "Cannot randomize topic name for field \"%s\" with final keyword",
                    topicNameField.getName());
        }
    }

    private void checkAnnotatedFieldType(
            Field field, Class<? extends Annotation> annotation, Class<?> expectedType) {
        if (!field.getType().isAssignableFrom(expectedType)) {
            throw new IllegalStateException(
                    String.format(
                            "Annotation @%s should only be used on field with type %s",
                            annotation.getSimpleName(), expectedType.getCanonicalName()));
        }
    }

    private boolean isKafkaKitParameter(ParameterContext parameterContext) {
        return parameterContext.getParameter().getType().isAssignableFrom(KafkaClientKit.class);
    }
}
