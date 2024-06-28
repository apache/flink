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

package org.apache.flink.table.planner.plan.rules.logical;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.immutables.value.Generated;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * {@code ImmutableFlinkMultiJoinOptimizeBushyRule} contains immutable implementation classes from
 * abstract value types defined as nested inside {@link FlinkFlinkMultiJoinOptimizeBushyRule}.
 * @see Config
 */
@Generated(from = "FlinkMultiJoinOptimizeBushyRule", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
final class ImmutableFlinkMultiJoinOptimizeBushyRule {
  private ImmutableFlinkMultiJoinOptimizeBushyRule() {}

  /**
   * Immutable implementation of {@link FlinkMultiJoinOptimizeBushyRule.Config}.
   * <p>
   * Use the builder to create immutable instances:
   * {@code ImmutableFlinkMultiJoinOptimizeBushyRule.Config.builder()}.
   * Use the static factory method to get the default singleton instance:
   * {@code ImmutableFlinkMultiJoinOptimizeBushyRule.Config.of()}.
   */
  @Generated(from = "FlinkMultiJoinOptimizeBushyRule.Config", generator = "Immutables")
  @Immutable
  @CheckReturnValue
  static final class Config implements FlinkMultiJoinOptimizeBushyRule.Config {
    private final RelBuilderFactory relBuilderFactory;
    private final @Nullable
    @org.checkerframework.checker.nullness.qual.Nullable String description;
    private final RelRule.OperandTransform operandSupplier;

    private Config() {
      this.description = null;
      this.relBuilderFactory = initShim.relBuilderFactory();
      this.operandSupplier = initShim.operandSupplier();
      this.initShim = null;
    }

    private Config(Builder builder) {
      this.description = builder.description;
      if (builder.relBuilderFactory != null) {
        initShim.withRelBuilderFactory(builder.relBuilderFactory);
      }
      if (builder.operandSupplier != null) {
        initShim.withOperandSupplier(builder.operandSupplier);
      }
      this.relBuilderFactory = initShim.relBuilderFactory();
      this.operandSupplier = initShim.operandSupplier();
      this.initShim = null;
    }

    private Config(
        RelBuilderFactory relBuilderFactory,
        @Nullable @org.checkerframework.checker.nullness.qual.Nullable String description,
        RelRule.OperandTransform operandSupplier) {
      this.relBuilderFactory = relBuilderFactory;
      this.description = description;
      this.operandSupplier = operandSupplier;
      this.initShim = null;
    }

    private static final byte STAGE_INITIALIZING = -1;
    private static final byte STAGE_UNINITIALIZED = 0;
    private static final byte STAGE_INITIALIZED = 1;
    @SuppressWarnings("Immutable")
    private transient volatile InitShim initShim = new InitShim();

    @Generated(from = "FlinkMultiJoinOptimizeBushyRule.Config", generator = "Immutables")
    private final class InitShim {
      private byte relBuilderFactoryBuildStage = STAGE_UNINITIALIZED;
      private RelBuilderFactory relBuilderFactory;

      RelBuilderFactory relBuilderFactory() {
        if (relBuilderFactoryBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
        if (relBuilderFactoryBuildStage == STAGE_UNINITIALIZED) {
          relBuilderFactoryBuildStage = STAGE_INITIALIZING;
          this.relBuilderFactory = Objects.requireNonNull(relBuilderFactoryInitialize(), "relBuilderFactory");
          relBuilderFactoryBuildStage = STAGE_INITIALIZED;
        }
        return this.relBuilderFactory;
      }

      void withRelBuilderFactory(RelBuilderFactory relBuilderFactory) {
        this.relBuilderFactory = relBuilderFactory;
        relBuilderFactoryBuildStage = STAGE_INITIALIZED;
      }

      private byte operandSupplierBuildStage = STAGE_UNINITIALIZED;
      private RelRule.OperandTransform operandSupplier;

      RelRule.OperandTransform operandSupplier() {
        if (operandSupplierBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
        if (operandSupplierBuildStage == STAGE_UNINITIALIZED) {
          operandSupplierBuildStage = STAGE_INITIALIZING;
          this.operandSupplier = Objects.requireNonNull(operandSupplierInitialize(), "operandSupplier");
          operandSupplierBuildStage = STAGE_INITIALIZED;
        }
        return this.operandSupplier;
      }

      void withOperandSupplier(RelRule.OperandTransform operandSupplier) {
        this.operandSupplier = operandSupplier;
        operandSupplierBuildStage = STAGE_INITIALIZED;
      }

      private String formatInitCycleMessage() {
        List<String> attributes = new ArrayList<>();
        if (relBuilderFactoryBuildStage == STAGE_INITIALIZING) attributes.add("relBuilderFactory");
        if (operandSupplierBuildStage == STAGE_INITIALIZING) attributes.add("operandSupplier");
        return "Cannot build Config, attribute initializers form cycle " + attributes;
      }
    }

    private RelBuilderFactory relBuilderFactoryInitialize() {
      return FlinkMultiJoinOptimizeBushyRule.Config.super.relBuilderFactory();
    }

    private RelRule.OperandTransform operandSupplierInitialize() {
      return FlinkMultiJoinOptimizeBushyRule.Config.super.operandSupplier();
    }

    /**
     * @return The value of the {@code relBuilderFactory} attribute
     */
    @Override
    public RelBuilderFactory relBuilderFactory() {
      InitShim shim = this.initShim;
      return shim != null
          ? shim.relBuilderFactory()
          : this.relBuilderFactory;
    }

    /**
     * @return The value of the {@code description} attribute
     */
    @Override
    public @Nullable
    @org.checkerframework.checker.nullness.qual.Nullable String description() {
      return description;
    }

    /**
     * @return The value of the {@code operandSupplier} attribute
     */
    @Override
    public RelRule.OperandTransform operandSupplier() {
      InitShim shim = this.initShim;
      return shim != null
          ? shim.operandSupplier()
          : this.operandSupplier;
    }

    /**
     * Copy the current immutable object by setting a value for the {@link FlinkMultiJoinOptimizeBushyRule.Config#relBuilderFactory() relBuilderFactory} attribute.
     * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for relBuilderFactory
     * @return A modified copy of the {@code this} object
     */
    public final Config withRelBuilderFactory(RelBuilderFactory value) {
      if (this.relBuilderFactory == value) return this;
      RelBuilderFactory newValue = Objects.requireNonNull(value, "relBuilderFactory");
      return validate(new Config(newValue, this.description, this.operandSupplier));
    }

    /**
     * Copy the current immutable object by setting a value for the {@link FlinkMultiJoinOptimizeBushyRule.Config#description() description} attribute.
     * An equals check used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for description (can be {@code null})
     * @return A modified copy of the {@code this} object
     */
    public final Config withDescription(@Nullable @org.checkerframework.checker.nullness.qual.Nullable String value) {
      if (Objects.equals(this.description, value)) return this;
      return validate(new Config(this.relBuilderFactory, value, this.operandSupplier));
    }

    /**
     * Copy the current immutable object by setting a value for the {@link FlinkMultiJoinOptimizeBushyRule.Config#operandSupplier() operandSupplier} attribute.
     * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for operandSupplier
     * @return A modified copy of the {@code this} object
     */
    public final Config withOperandSupplier(RelRule.OperandTransform value) {
      if (this.operandSupplier == value) return this;
      RelRule.OperandTransform newValue = Objects.requireNonNull(value, "operandSupplier");
      return validate(new Config(this.relBuilderFactory, this.description, newValue));
    }

    /**
     * This instance is equal to all instances of {@code Config} that have equal attribute values.
     * @return {@code true} if {@code this} is equal to {@code another} instance
     */
    @Override
    public boolean equals(@Nullable Object another) {
      if (this == another) return true;
      return another instanceof Config
          && equalTo((Config) another);
    }

    private boolean equalTo(Config another) {
      return relBuilderFactory.equals(another.relBuilderFactory)
          && Objects.equals(description, another.description)
          && operandSupplier.equals(another.operandSupplier);
    }

    /**
     * Computes a hash code from attributes: {@code relBuilderFactory}, {@code description}, {@code operandSupplier}.
     * @return hashCode value
     */
    @Override
    public int hashCode() {
      @Var int h = 5381;
      h += (h << 5) + relBuilderFactory.hashCode();
      h += (h << 5) + Objects.hashCode(description);
      h += (h << 5) + operandSupplier.hashCode();
      return h;
    }

    /**
     * Prints the immutable value {@code Config} with attribute values.
     * @return A string representation of the value
     */
    @Override
    public String toString() {
      return MoreObjects.toStringHelper("Config")
          .omitNullValues()
          .add("relBuilderFactory", relBuilderFactory)
          .add("description", description)
          .add("operandSupplier", operandSupplier)
          .toString();
    }

    private static final Config INSTANCE = validate(new Config());

    /**
     * Returns the default immutable singleton value of {@code Config}
     * @return An immutable instance of Config
     */
    public static Config of() {
      return INSTANCE;
    }

    private static Config validate(Config instance) {
      return INSTANCE != null && INSTANCE.equalTo(instance) ? INSTANCE : instance;
    }

    /**
     * Creates an immutable copy of a {@link FlinkMultiJoinOptimizeBushyRule.Config} value.
     * Uses accessors to get values to initialize the new immutable instance.
     * If an instance is already immutable, it is returned as is.
     * @param instance The instance to copy
     * @return A copied immutable Config instance
     */
    public static Config copyOf(FlinkMultiJoinOptimizeBushyRule.Config instance) {
      if (instance instanceof Config) {
        return (Config) instance;
      }
      return Config.builder()
          .from(instance)
          .build();
    }

    /**
     * Creates a builder for {@link Config Config}.
     * <pre>
     * ImmutableFlinkMultiJoinOptimizeBushyRule.Config.builder()
     *    .withRelBuilderFactory(org.apache.calcite.tools.RelBuilderFactory) // optional {@link FlinkMultiJoinOptimizeBushyRule.Config#relBuilderFactory() relBuilderFactory}
     *    .withDescription(@org.checkerframework.checker.nullness.qual.Nullable String | null) // nullable {@link FlinkMultiJoinOptimizeBushyRule.Config#description() description}
     *    .withOperandSupplier(org.apache.calcite.plan.RelRule.OperandTransform) // optional {@link FlinkMultiJoinOptimizeBushyRule.Config#operandSupplier() operandSupplier}
     *    .build();
     * </pre>
     * @return A new Config builder
     */
    public static Builder builder() {
      return new Builder();
    }

    /**
     * Builds instances of type {@link Config Config}.
     * Initialize attributes and then invoke the {@link #build()} method to create an
     * immutable instance.
     * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
     * but instead used immediately to create instances.</em>
     */
    @Generated(from = "FlinkMultiJoinOptimizeBushyRule.Config", generator = "Immutables")
    @NotThreadSafe
    public static final class Builder {
      private @Nullable RelBuilderFactory relBuilderFactory;
      private @Nullable
      @org.checkerframework.checker.nullness.qual.Nullable String description;
      private @Nullable RelRule.OperandTransform operandSupplier;

      private Builder() {
      }

      /**
       * Fill a builder with attribute values from the provided {@code org.apache.calcite.plan.RelRule.Config} instance.
       * @param instance The instance from which to copy values
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder from(RelRule.Config instance) {
        Objects.requireNonNull(instance, "instance");
        from((Object) instance);
        return this;
      }

      /**
       * Fill a builder with attribute values from the provided {@code org.apache.calcite.rel.rules.FlinkMultiJoinOptimizeBushyRule.Config} instance.
       * @param instance The instance from which to copy values
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder from(FlinkMultiJoinOptimizeBushyRule.Config instance) {
        Objects.requireNonNull(instance, "instance");
        from((Object) instance);
        return this;
      }

      private void from(Object object) {
        if (object instanceof RelRule.Config) {
          RelRule.Config instance = (RelRule.Config) object;
          withRelBuilderFactory(instance.relBuilderFactory());
          withOperandSupplier(instance.operandSupplier());
          @Nullable @org.checkerframework.checker.nullness.qual.Nullable String descriptionValue = instance.description();
          if (descriptionValue != null) {
            withDescription(descriptionValue);
          }
        }
      }

      /**
       * Initializes the value for the {@link FlinkMultiJoinOptimizeBushyRule.Config#relBuilderFactory() relBuilderFactory} attribute.
       * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link FlinkMultiJoinOptimizeBushyRule.Config#relBuilderFactory() relBuilderFactory}.</em>
       * @param relBuilderFactory The value for relBuilderFactory 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder withRelBuilderFactory(RelBuilderFactory relBuilderFactory) {
        this.relBuilderFactory = Objects.requireNonNull(relBuilderFactory, "relBuilderFactory");
        return this;
      }

      /**
       * Initializes the value for the {@link FlinkMultiJoinOptimizeBushyRule.Config#description() description} attribute.
       * @param description The value for description (can be {@code null})
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder withDescription(@Nullable @org.checkerframework.checker.nullness.qual.Nullable String description) {
        this.description = description;
        return this;
      }

      /**
       * Initializes the value for the {@link FlinkMultiJoinOptimizeBushyRule.Config#operandSupplier() operandSupplier} attribute.
       * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link FlinkMultiJoinOptimizeBushyRule.Config#operandSupplier() operandSupplier}.</em>
       * @param operandSupplier The value for operandSupplier 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder withOperandSupplier(RelRule.OperandTransform operandSupplier) {
        this.operandSupplier = Objects.requireNonNull(operandSupplier, "operandSupplier");
        return this;
      }

      /**
       * Builds a new {@link Config Config}.
       * @return An immutable instance of Config
       * @throws IllegalStateException if any required attributes are missing
       */
      public Config build() {
        return Config.validate(new Config(this));
      }
    }
  }
}
