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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig.ClosureCleanerLevel;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;

/** The {@link ConfigOption configuration options} for job execution. */
@PublicEvolving
public class PipelineOptions {

    /** The job name used for printing and logging. */
    public static final ConfigOption<String> NAME =
            key("pipeline.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The job name used for printing and logging.");

    /**
     * A list of jar files that contain the user-defined function (UDF) classes and all classes used
     * from within the UDFs.
     */
    public static final ConfigOption<List<String>> JARS =
            key("pipeline.jars")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "A semicolon-separated list of the jars to package with the job jars to be sent to the"
                                    + " cluster. These have to be valid paths.");
    /**
     * A list of URLs that are added to the classpath of each user code classloader of the program.
     * Paths must specify a protocol (e.g. file://) and be accessible on all nodes
     */
    public static final ConfigOption<List<String>> CLASSPATHS =
            key("pipeline.classpaths")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "A semicolon-separated list of the classpaths to package with the job jars to be sent to"
                                    + " the cluster. These have to be valid URLs.");

    public static final ConfigOption<Boolean> AUTO_GENERATE_UIDS =
            key("pipeline.auto-generate-uids")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "When auto-generated UIDs are disabled, users are forced to manually specify UIDs on DataStream applications.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "It is highly recommended that users specify UIDs before deploying to"
                                                    + " production since they are used to match state in savepoints to operators"
                                                    + " in a job. Because auto-generated ID's are likely to change when modifying"
                                                    + " a job, specifying custom IDs allow an application to evolve over time"
                                                    + " without discarding state.")
                                    .build());

    /**
     * An option to control whether Flink is automatically registering all types in the user
     * programs with Kryo.
     *
     * @deprecated The config is deprecated because it's only used in DataSet API. All Flink DataSet
     *     APIs are deprecated since Flink 1.18 and will be removed in a future Flink major version.
     *     You can still build your application in DataSet, but you should move to either the
     *     DataStream and/or Table API.
     * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
     *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet
     *     API</a>
     */
    @Deprecated
    public static final ConfigOption<Boolean> AUTO_TYPE_REGISTRATION =
            key("pipeline.auto-type-registration")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Controls whether Flink is automatically registering all types in the user programs"
                                    + " with Kryo.");

    public static final ConfigOption<Duration> AUTO_WATERMARK_INTERVAL =
            key("pipeline.auto-watermark-interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(200))
                    .withDescription(
                            "The interval of the automatic watermark emission. Watermarks are used throughout"
                                    + " the streaming system to keep track of the progress of time. They are used, for example,"
                                    + " for time based windowing.");

    public static final ConfigOption<ClosureCleanerLevel> CLOSURE_CLEANER_LEVEL =
            key("pipeline.closure-cleaner-level")
                    .enumType(ClosureCleanerLevel.class)
                    .defaultValue(ClosureCleanerLevel.RECURSIVE)
                    .withDescription("Configures the mode in which the closure cleaner works.");

    public static final ConfigOption<Boolean> FORCE_AVRO =
            key("pipeline.force-avro")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Forces Flink to use the Apache Avro serializer for POJOs.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Important: Make sure to include the %s module.",
                                            code("flink-avro"))
                                    .build());

    public static final ConfigOption<Boolean> FORCE_KRYO =
            key("pipeline.force-kryo")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If enabled, forces TypeExtractor to use Kryo serializer for POJOS even though we could"
                                    + " analyze as POJO. In some cases this might be preferable. For example, when using interfaces"
                                    + " with subclasses that cannot be analyzed as POJO.");

    public static final ConfigOption<Boolean> GENERIC_TYPES =
            key("pipeline.generic-types")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If the use of generic types is disabled, Flink will throw an %s whenever it encounters"
                                                    + " a data type that would go through Kryo for serialization.",
                                            code("UnsupportedOperationException"))
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "Disabling generic types can be helpful to eagerly find and eliminate the use of types"
                                                    + " that would go through Kryo serialization during runtime. Rather than checking types"
                                                    + " individually, using this option will throw exceptions eagerly in the places where generic"
                                                    + " types are used.")
                                    .linebreak()
                                    .linebreak()
                                    .text(
                                            "We recommend to use this option only during development and pre-production"
                                                    + " phases, not during actual production use. The application program and/or the input data may be"
                                                    + " such that new, previously unseen, types occur at some point. In that case, setting this option"
                                                    + " would cause the program to fail.")
                                    .build());

    public static final ConfigOption<Map<String, String>> GLOBAL_JOB_PARAMETERS =
            key("pipeline.global-job-parameters")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Register a custom, serializable user configuration object. The configuration can be "
                                    + " accessed in operators");

    public static final ConfigOption<Map<String, String>> PARALLELISM_OVERRIDES =
            key("pipeline.jobvertex-parallelism-overrides")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "A parallelism override map (jobVertexId -> parallelism) which will be used to update"
                                    + " the parallelism of the corresponding job vertices of submitted JobGraphs.");

    public static final ConfigOption<Integer> MAX_PARALLELISM =
            key("pipeline.max-parallelism")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The program-wide maximum parallelism used for operators which haven't specified a"
                                    + " maximum parallelism. The maximum parallelism specifies the upper limit for dynamic scaling and"
                                    + " the number of key groups used for partitioned state."
                                    + " Changing the value explicitly when recovery from original job will lead to state incompatibility."
                                    + " Must be less than or equal to 32768.");

    public static final ConfigOption<Boolean> OBJECT_REUSE =
            key("pipeline.object-reuse")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When enabled objects that Flink internally uses for deserialization and passing"
                                    + " data to user-code functions will be reused. Keep in mind that this can lead to bugs when the"
                                    + " user-code function of an operation is not aware of this behaviour.");

    public static final ConfigOption<List<String>> KRYO_DEFAULT_SERIALIZERS =
            key("pipeline.default-kryo-serializers")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Semicolon separated list of pairs of class names and Kryo serializers class names to be used"
                                                    + " as Kryo default serializers")
                                    .linebreak()
                                    .linebreak()
                                    .text("Example:")
                                    .linebreak()
                                    .add(
                                            TextElement.code(
                                                    "class:org.example.ExampleClass,serializer:org.example.ExampleSerializer1;"
                                                            + " class:org.example.ExampleClass2,serializer:org.example.ExampleSerializer2"))
                                    .build());

    public static final ConfigOption<List<String>> KRYO_REGISTERED_CLASSES =
            key("pipeline.registered-kryo-types")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Semicolon separated list of types to be registered with the serialization stack. If the type"
                                                    + " is eventually serialized as a POJO, then the type is registered with the POJO serializer. If the"
                                                    + " type ends up being serialized with Kryo, then it will be registered at Kryo to make"
                                                    + " sure that only tags are written.")
                                    .build());

    public static final ConfigOption<List<String>> POJO_REGISTERED_CLASSES =
            key("pipeline.registered-pojo-types")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Semicolon separated list of types to be registered with the serialization stack. If the type"
                                                    + " is eventually serialized as a POJO, then the type is registered with the POJO serializer. If the"
                                                    + " type ends up being serialized with Kryo, then it will be registered at Kryo to make"
                                                    + " sure that only tags are written.")
                                    .build());

    public static final ConfigOption<Boolean> OPERATOR_CHAINING =
            key("pipeline.operator-chaining.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDeprecatedKeys("pipeline.operator-chaining")
                    .withDescription(
                            "Operator chaining allows non-shuffle operations to be co-located in the same thread "
                                    + "fully avoiding serialization and de-serialization.");

    public static final ConfigOption<Boolean>
            OPERATOR_CHAINING_CHAIN_OPERATORS_WITH_DIFFERENT_MAX_PARALLELISM =
                    key("pipeline.operator-chaining.chain-operators-with-different-max-parallelism")
                            .booleanType()
                            .defaultValue(true)
                            .withDescription(
                                    "Operators with different max parallelism can be chained together. Default behavior may prevent rescaling when the AdaptiveScheduler is used.");

    public static final ConfigOption<List<String>> CACHED_FILES =
            key("pipeline.cached-files")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Files to be registered at the distributed cache under the given name. The files will be "
                                                    + "accessible from any user-defined function in the (distributed) runtime under a local path. "
                                                    + "Files may be local files (which will be distributed via BlobServer), or files in a distributed "
                                                    + "file system. The runtime will copy the files temporarily to a local cache, if needed.")
                                    .linebreak()
                                    .linebreak()
                                    .text("Example:")
                                    .linebreak()
                                    .add(
                                            TextElement.code(
                                                    "name:file1,path:'file:///tmp/file1';name:file2,path:'hdfs:///tmp/file2'"))
                                    .build());

    public static final ConfigOption<VertexDescriptionMode> VERTEX_DESCRIPTION_MODE =
            key("pipeline.vertex-description-mode")
                    .enumType(VertexDescriptionMode.class)
                    .defaultValue(VertexDescriptionMode.TREE)
                    .withDescription("The mode how we organize description of a job vertex.");

    /** The mode how we organize description of a vertex. */
    @PublicEvolving
    public enum VertexDescriptionMode {
        /** Organizes the description in a multi line tree mode. */
        TREE,
        /** Organizes the description in a single line cascading mode, which is similar to name. */
        CASCADING
    }

    public static final ConfigOption<Boolean> VERTEX_NAME_INCLUDE_INDEX_PREFIX =
            key("pipeline.vertex-name-include-index-prefix")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether name of vertex includes topological index or not. "
                                    + "When it is true, the name will have a prefix of index of the vertex, like '[vertex-0]Source: source'. It is false by default");

    /** Will be removed in future Flink releases. */
    public static final ConfigOption<Boolean> ALLOW_UNALIGNED_SOURCE_SPLITS =
            key("pipeline.watermark-alignment.allow-unaligned-source-splits")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If watermark alignment is used, sources with multiple splits will "
                                    + "attempt to pause/resume split readers to avoid watermark "
                                    + "drift of source splits. "
                                    + "However, if split readers don't support pause/resume, an "
                                    + "UnsupportedOperationException will be thrown when there is "
                                    + "an attempt to pause/resume. To allow use of split readers that "
                                    + "don't support pause/resume and, hence, to allow unaligned splits "
                                    + "while still using watermark alignment, set this parameter to true. "
                                    + "The default value is false. Note: This parameter may be "
                                    + "removed in future releases.");
}
