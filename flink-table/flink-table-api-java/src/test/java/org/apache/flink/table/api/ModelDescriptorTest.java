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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CatalogModel;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ModelDescriptor}. */
class ModelDescriptorTest {

    private static final ConfigOption<Boolean> OPTION_A =
            ConfigOptions.key("a").booleanType().noDefaultValue();

    private static final ConfigOption<Integer> OPTION_B =
            ConfigOptions.key("b").intType().noDefaultValue();

    private static final ConfigOption<String> TASK_OPTION =
            ConfigOptions.key("task").stringType().noDefaultValue();

    @Test
    void testBasic() {
        final Schema inputSchema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.BIGINT())
                        .build();

        final Schema outputSchema =
                Schema.newBuilder().column("response", DataTypes.DOUBLE()).build();

        final ModelDescriptor descriptor =
                ModelDescriptor.forProvider("test-provider")
                        .inputSchema(inputSchema)
                        .outputSchema(outputSchema)
                        .comment("Test Model Comment")
                        .build();

        assertThat(descriptor.getInputSchema()).isPresent();
        assertThat(descriptor.getInputSchema().get()).isEqualTo(inputSchema);

        assertThat(descriptor.getOutputSchema()).isPresent();
        assertThat(descriptor.getOutputSchema().get()).isEqualTo(outputSchema);

        assertThat(descriptor.getOptions()).hasSize(1);
        assertThat(descriptor.getOptions()).containsEntry("provider", "test-provider");

        assertThat(descriptor.getComment().orElse(null)).isEqualTo("Test Model Comment");
    }

    @Test
    void testNoSchemas() {
        final ModelDescriptor descriptor = ModelDescriptor.forProvider("test-provider").build();

        assertThat(descriptor.getInputSchema()).isNotPresent();
        assertThat(descriptor.getOutputSchema()).isNotPresent();
    }

    @Test
    void testOptions() {
        final ModelDescriptor descriptor =
                ModelDescriptor.forProvider("test-provider")
                        .inputSchema(Schema.newBuilder().build())
                        .outputSchema(Schema.newBuilder().build())
                        .option(OPTION_A, false)
                        .option(OPTION_B, 42)
                        .option("task", "embedding")
                        .build();

        assertThat(descriptor.getOptions()).hasSize(4);
        assertThat(descriptor.getOptions()).containsEntry("provider", "test-provider");
        assertThat(descriptor.getOptions()).containsEntry("a", "false");
        assertThat(descriptor.getOptions()).containsEntry("b", "42");
        assertThat(descriptor.getOptions()).containsEntry("task", "embedding");
    }

    @Test
    void testToCatalogModel() {
        final Schema inputSchema =
                Schema.newBuilder()
                        .column("feature1", DataTypes.DOUBLE())
                        .column("feature2", DataTypes.DOUBLE())
                        .build();

        final Schema outputSchema =
                Schema.newBuilder().column("response", DataTypes.STRING()).build();

        final ModelDescriptor descriptor =
                ModelDescriptor.forProvider("test-provider")
                        .inputSchema(inputSchema)
                        .outputSchema(outputSchema)
                        .option("task", "classification")
                        .comment("Test Model")
                        .build();

        final CatalogModel catalogModel = descriptor.toCatalogModel();

        assertThat(catalogModel.getInputSchema()).isEqualTo(inputSchema);
        assertThat(catalogModel.getOutputSchema()).isEqualTo(outputSchema);
        assertThat(catalogModel.getOptions()).containsEntry("provider", "test-provider");
        assertThat(catalogModel.getOptions()).containsEntry("task", "classification");
        assertThat(catalogModel.getComment()).isEqualTo("Test Model");
    }

    @Test
    void testToCatalogModelMissingInputSchema() {
        final ModelDescriptor descriptor =
                ModelDescriptor.forProvider("test-provider")
                        .outputSchema(
                                Schema.newBuilder().column("output", DataTypes.STRING()).build())
                        .build();

        assertThatThrownBy(descriptor::toCatalogModel)
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Input schema missing in ModelDescriptor. Input schema cannot be null.");
    }

    @Test
    void testToCatalogModelMissingOutputSchema() {
        final ModelDescriptor descriptor =
                ModelDescriptor.forProvider("test-provider")
                        .inputSchema(
                                Schema.newBuilder().column("input", DataTypes.STRING()).build())
                        .build();

        assertThatThrownBy(descriptor::toCatalogModel)
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Output schema missing in ModelDescriptor. Output schema cannot be null.");
    }

    @Test
    void testToString() {
        final Schema inputSchema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.INT())
                        .build();

        final Schema outputSchema =
                Schema.newBuilder().column("prediction", DataTypes.DOUBLE()).build();

        final ModelDescriptor modelDescriptor =
                ModelDescriptor.forProvider("test-provider")
                        .inputSchema(inputSchema)
                        .outputSchema(outputSchema)
                        .option(OPTION_A, true)
                        .option("task", "regression")
                        .comment("Test Model Comment")
                        .build();

        assertThat(modelDescriptor.toString())
                .isEqualTo(
                        "(\n"
                                + "  `f0` STRING,\n"
                                + "  `f1` INT\n"
                                + ")\n"
                                + "(\n"
                                + "  `prediction` DOUBLE\n"
                                + ")\n"
                                + "COMMENT 'Test Model Comment'\n"
                                + "WITH (\n"
                                + "  'a' = 'true',\n"
                                + "  'task' = 'regression',\n"
                                + "  'provider' = 'test-provider'\n"
                                + ")");
    }

    @Test
    void testToBuilder() {
        final Schema inputSchema = Schema.newBuilder().column("f0", DataTypes.STRING()).build();
        final Schema outputSchema =
                Schema.newBuilder().column("prediction", DataTypes.DOUBLE()).build();

        final ModelDescriptor original =
                ModelDescriptor.forProvider("test-provider")
                        .inputSchema(inputSchema)
                        .outputSchema(outputSchema)
                        .option("task", "classification")
                        .comment("Original Comment")
                        .build();

        final ModelDescriptor modified =
                original.toBuilder()
                        .option("new-option", "new-value")
                        .comment("Modified Comment")
                        .build();

        assertThat(original.getComment().orElse(null)).isEqualTo("Original Comment");
        assertThat(original.getOptions()).doesNotContainKey("new-option");
        assertThat(modified.getComment().orElse(null)).isEqualTo("Modified Comment");
        assertThat(modified.getOptions()).containsEntry("new-option", "new-value");
        assertThat(modified.getOptions()).containsEntry("task", "classification");
        assertThat(modified.getOptions()).containsEntry("provider", "test-provider");

        // Schemas should be preserved
        assertThat(modified.getInputSchema()).isEqualTo(original.getInputSchema());
        assertThat(modified.getOutputSchema()).isEqualTo(original.getOutputSchema());
    }

    @Test
    void testEqualsAndHashCode() {
        final Schema inputSchema = Schema.newBuilder().column("f0", DataTypes.STRING()).build();
        final Schema outputSchema =
                Schema.newBuilder().column("prediction", DataTypes.DOUBLE()).build();

        final ModelDescriptor descriptor1 =
                ModelDescriptor.forProvider("test-provider")
                        .inputSchema(inputSchema)
                        .outputSchema(outputSchema)
                        .option("task", "regression")
                        .comment("Test Comment")
                        .build();

        final ModelDescriptor descriptor2 =
                ModelDescriptor.forProvider("test-provider")
                        .inputSchema(inputSchema)
                        .outputSchema(outputSchema)
                        .option("task", "regression")
                        .comment("Test Comment")
                        .build();

        final ModelDescriptor descriptor3 =
                ModelDescriptor.forProvider("different-provider")
                        .inputSchema(inputSchema)
                        .outputSchema(outputSchema)
                        .option("task", "regression")
                        .comment("Test Comment")
                        .build();

        assertThat(descriptor1).isEqualTo(descriptor2);
        assertThat(descriptor1).isNotEqualTo(descriptor3);
        assertThat(descriptor1).isNotEqualTo(null);
        assertThat(descriptor1).isNotEqualTo("not a model descriptor");

        assertThat(descriptor1.hashCode()).isEqualTo(descriptor2.hashCode());
    }

    @Test
    void testBuilderExceptions() {
        assertThatThrownBy(() -> ModelDescriptor.forProvider(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Model descriptors require a provider value.");

        final ModelDescriptor.Builder builder = ModelDescriptor.forProvider("test-provider");

        assertThatThrownBy(() -> builder.option((ConfigOption<String>) null, "value"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Config option must not be null.");

        assertThatThrownBy(() -> builder.option(TASK_OPTION, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Value must not be null.");

        assertThatThrownBy(() -> builder.option((String) null, "value"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Key must not be null.");

        assertThatThrownBy(() -> builder.option("key", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Value must not be null.");
    }

    @Test
    void testNullComment() {
        final ModelDescriptor descriptor =
                ModelDescriptor.forProvider("test-provider").comment(null).build();

        assertThat(descriptor.getComment()).isNotPresent();
    }
}
