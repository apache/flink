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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Describes a {@link CatalogModel} representing a model.
 *
 * <p>A {@link ModelDescriptor} is a template for creating a {@link CatalogModel} instance. It
 * closely resembles the "CREATE MODEL" SQL DDL statement, containing input schema, output schema,
 * and other characteristics.
 *
 * <p>This can be used to register a Model in the Table API.
 */
@PublicEvolving
public class ModelDescriptor {
    private final @Nullable Schema inputSchema;
    private final @Nullable Schema outputSchema;
    private final Map<String, String> modelOptions;
    private final @Nullable String comment;

    protected ModelDescriptor(
            @Nullable Schema inputSchema,
            @Nullable Schema outputSchema,
            Map<String, String> modelOptions,
            @Nullable String comment) {
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
        this.modelOptions = modelOptions;
        this.comment = comment;
    }

    /** Converts this descriptor into a {@link CatalogModel}. */
    public CatalogModel toCatalogModel() {
        final Schema inputSchema =
                getInputSchema()
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                "Input schema missing in ModelDescriptor. Input schema cannot be null."));
        final Schema outputSchema =
                getOutputSchema()
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                "Output schema missing in ModelDescriptor. Output schema cannot be null."));
        return CatalogModel.of(inputSchema, outputSchema, modelOptions, comment);
    }

    /** Converts this immutable instance into a mutable {@link Builder}. */
    public Builder toBuilder() {
        return new Builder(this);
    }

    // ---------------------------------------------------------------------------------------------

    /** Returns a map of string-based model options. */
    Map<String, String> getOptions() {
        return Map.copyOf(modelOptions);
    }

    /** Get the unresolved input schema of the model. */
    Optional<Schema> getInputSchema() {
        return Optional.ofNullable(inputSchema);
    }

    /** Get the unresolved output schema of the model. */
    Optional<Schema> getOutputSchema() {
        return Optional.ofNullable(outputSchema);
    }

    /** Get comment of the model. */
    Optional<String> getComment() {
        return Optional.ofNullable(comment);
    }

    // ---------------------------------------------------------------------------------------------

    /**
     * Creates a new {@link Builder} for the model with the given provider option.
     *
     * @param provider string value of provider for the model.
     */
    public static Builder forProvider(String provider) {
        Preconditions.checkNotNull(provider, "Model descriptors require a provider value.");
        final Builder descriptorBuilder = new Builder();
        descriptorBuilder.option(FactoryUtil.PROVIDER, provider);
        return descriptorBuilder;
    }

    @Override
    public String toString() {
        final String serializedOptions =
                modelOptions.entrySet().stream()
                        .map(
                                entry ->
                                        String.format(
                                                "  '%s' = '%s'",
                                                EncodingUtils.escapeSingleQuotes(entry.getKey()),
                                                EncodingUtils.escapeSingleQuotes(entry.getValue())))
                        .collect(Collectors.joining(String.format(",%n")));

        return String.format(
                "%s%n%s%nCOMMENT '%s'%nWITH (%n%s%n)",
                inputSchema != null ? inputSchema : "",
                outputSchema != null ? outputSchema : "",
                comment != null ? comment : "",
                serializedOptions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ModelDescriptor that = (ModelDescriptor) o;
        return Objects.equals(inputSchema, that.inputSchema)
                && Objects.equals(outputSchema, that.outputSchema)
                && modelOptions.equals(that.modelOptions)
                && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputSchema, outputSchema, modelOptions, comment);
    }

    // ---------------------------------------------------------------------------------------------

    /** Builder for {@link ModelDescriptor}. */
    @PublicEvolving
    public static class Builder {
        private @Nullable Schema inputSchema;
        private @Nullable Schema outputSchema;
        private final Map<String, String> modelOptions;
        private @Nullable String comment;

        private Builder() {
            this.modelOptions = new HashMap<>();
        }

        private Builder(ModelDescriptor descriptor) {
            this.inputSchema = descriptor.getInputSchema().orElse(null);
            this.outputSchema = descriptor.getOutputSchema().orElse(null);
            this.modelOptions = new HashMap<>(descriptor.getOptions());
            this.comment = descriptor.getComment().orElse(null);
        }

        /** Sets the given option on the model. */
        public <T> Builder option(ConfigOption<T> configOption, T value) {
            Preconditions.checkNotNull(configOption, "Config option must not be null.");
            Preconditions.checkNotNull(value, "Value must not be null.");
            modelOptions.put(
                    configOption.key(), ConfigurationUtils.convertValue(value, String.class));
            return this;
        }

        /**
         * Sets the given option on the model.
         *
         * <p>Option keys must be fully specified.
         *
         * <p>Example:
         *
         * <pre>{@code
         * ModelDescriptor.forProvider("OPENAI")
         *   .inputSchema(inputSchema)
         *   .outputSchema(outputSchema)
         *   .option("task", "regression")
         *   .build();
         * }</pre>
         */
        public Builder option(String key, String value) {
            Preconditions.checkNotNull(key, "Key must not be null.");
            Preconditions.checkNotNull(value, "Value must not be null.");
            this.modelOptions.put(key, value);
            return this;
        }

        /** Define the input schema of the {@link ModelDescriptor}. */
        public Builder inputSchema(@Nullable Schema inputSchema) {
            this.inputSchema = inputSchema;
            return this;
        }

        /** Define the output schema of the {@link ModelDescriptor}. */
        public Builder outputSchema(@Nullable Schema outputSchema) {
            this.outputSchema = outputSchema;
            return this;
        }

        /** Define the comment for this model. */
        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        /** Returns an immutable instance of {@link ModelDescriptor}. */
        public ModelDescriptor build() {
            return new ModelDescriptor(inputSchema, outputSchema, modelOptions, comment);
        }
    }
}
