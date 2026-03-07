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

package org.apache.flink.protobuf.registry.confluent.dynamic;

import com.github.os72.protocjar.Protoc;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.type.DecimalProto;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/** Service to expose the functionality of the Protoc compiler. */
public class ProtoCompiler {

    private final String classSuffix;
    private final Path parentDir;
    private final Path protosDir;
    private final Path classesDir;
    private final String protocVersion;

    private static final String CONFLUENT = "confluent";
    private static final String META_PROTO = String.format("%s/%s", CONFLUENT, "meta.proto");
    private static final String DECIMAL_PROTO =
            String.format("%s/%s/%s", CONFLUENT, "type", "decimal.proto");
    // TODO: Hava constructor that can fetch the protocVersion from project properties
    private static final String DEFAULT_PROTOC_VERSION = "3.21.7";
    private static final List<String> DEFAULT_INCLUDES =
            Arrays.asList(
                    "/" + META_PROTO,
                    "/" + DECIMAL_PROTO,
                    "/google/protobuf/any.proto",
                    "/google/protobuf/api.proto",
                    "/google/protobuf/descriptor.proto",
                    "/google/protobuf/duration.proto",
                    "/google/protobuf/empty.proto",
                    "/google/protobuf/field_mask.proto",
                    "/google/protobuf/source_context.proto",
                    "/google/protobuf/struct.proto",
                    "/google/protobuf/timestamp.proto",
                    "/google/protobuf/type.proto",
                    "/google/protobuf/wrappers.proto");

    /**
     * @param parentDir The directory where the proto files and generated classes will be written to
     * @param protocVersion The version of protoc to use
     * @param classSuffix The suffix to append to the generated class name
     * @param customIncludes An optional list of resource URLs to copy to the parent directory, and
     *     include when invoking protoc
     * @param useDefaultIncludes Whether to include the default set of proto descriptors from {@link
     *     ProtoCompiler#DEFAULT_INCLUDES}
     */
    public ProtoCompiler(
            Path parentDir,
            String protocVersion,
            String classSuffix,
            boolean useDefaultIncludes,
            String... customIncludes) {
        this.classSuffix = classSuffix;
        this.parentDir = parentDir;
        this.protosDir = createChildDir(parentDir, "protos");
        this.classesDir = createChildDir(parentDir, "classes");
        this.protocVersion = protocVersion;
        List<String> resolvedIncludes = new ArrayList<>();
        if (useDefaultIncludes) {
            resolvedIncludes.addAll(DEFAULT_INCLUDES);
        }
        if (customIncludes != null && customIncludes.length > 0) {
            resolvedIncludes.addAll(Arrays.asList(customIncludes));
        }
        for (String include : resolvedIncludes) {
            copyIncludedProto(include);
        }
    }

    public ProtoCompiler(
            String protocVersion,
            String classSuffix,
            boolean useDefaultIncludes,
            String... customIncludes) {
        this(createParentDir(), protocVersion, classSuffix, useDefaultIncludes, customIncludes);
    }

    public ProtoCompiler(boolean useDefaultIncludes, String... customIncludes) {
        this(DEFAULT_PROTOC_VERSION, generateClassSuffix(), useDefaultIncludes, customIncludes);
    }

    public ProtoCompiler(String classSuffix, boolean useDefaultIncludes, String... customIncludes) {
        this(DEFAULT_PROTOC_VERSION, classSuffix, useDefaultIncludes, customIncludes);
    }

    /**
     * Given a protobuf schema this method will: 1. Write the schema to a .proto file in a temporary
     * directory 2. Generate the Java class definition using protoc 3. Compile a Java class from the
     * Java source file 4. Load the class into the JVM
     *
     * @return the Class of the generated message type
     */
    public Class generateMessageClass(ProtobufSchema protobufSchema, Integer schemaId) {
        String suffixedProtoClassName = suffixedProtoClassName(protobufSchema, schemaId);
        Path schemaFilePath = writeProto(protobufSchema, suffixedProtoClassName);

        String indexedClassName = schemaFilePath.getFileName().toString().replace(".proto", "");
        Path javaOutDir = generateJavaClassDef(schemaFilePath, indexedClassName);

        String packageName = protobufSchema.rawSchema().getPackageName();
        Path javaClassDefPath = getJavaClassDefPath(javaOutDir, packageName, indexedClassName);

        String topLevelClassName = packageName + "." + indexedClassName;
        compileJavaClass(javaClassDefPath, topLevelClassName);

        String nestedClassName = topLevelClassName + "$" + getClassNameFromProto(protobufSchema);
        return loadClass(javaOutDir, topLevelClassName, nestedClassName);
    }

    private Class loadClass(Path javaOutDir, String topLevelClassName, String nestedClassName) {
        try {
            URL classUrl = javaOutDir.toFile().toURI().toURL();
            URLClassLoader classLoader =
                    URLClassLoader.newInstance(
                            new URL[] {classUrl}, Thread.currentThread().getContextClassLoader());
            Thread.currentThread().setContextClassLoader(classLoader);
            classLoader.loadClass(topLevelClassName);
            return Class.forName(nestedClassName, true, classLoader);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load class " + nestedClassName, e);
        }
    }

    private void compileJavaClass(Path javaClassDefPath, String topLevelClassName) {
        // Tried using {@link PbUtils}'s existing compiler but was getting errors.
        // Assignment conversion not possible from type "java.lang.Object" to type
        // "com.google.protobuf.Descriptors$Descriptor"
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
        int result = compiler.run(null, outputStream, errorStream, javaClassDefPath.toString());
        if (result != 0) {
            String msg =
                    String.format(
                            "Failed to compile class %s: err=%s; out=%s",
                            topLevelClassName, errorStream, outputStream);
            throw new RuntimeException(msg);
        }
    }

    private Path getJavaClassDefPath(Path javaOutDir, String packageName, String className) {
        if (packageName == null || packageName.isEmpty()) {
            return javaOutDir.resolve(className + ".java");
        } else {
            return javaOutDir.resolve(packageName.replace(".", "/")).resolve(className + ".java");
        }
    }

    private Path generateJavaClassDef(Path schemaFilePath, String className) {
        Path javaOutDir = createChildDir(classesDir, className);
        String[] args = {
            "-I" + parentDir.toString(), // include custom types
            "-v" + protocVersion,
            "--java_out=" + javaOutDir.toString(),
            "--proto_path=" + protosDir.toString(),
            schemaFilePath.toString()
        };
        try {
            int result = Protoc.runProtoc(args);
            if (result != 0) {
                throw new RuntimeException("Failed to run protoc for " + schemaFilePath);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to run protoc for " + schemaFilePath, e);
        }
        return javaOutDir;
    }

    private String suffixedProtoClassName(ProtobufSchema protobufSchema, Integer schemaId) {
        StringBuilder sb = new StringBuilder();
        sb.append(getClassNameFromProto(protobufSchema));
        if (schemaId != null) {
            sb.append("_");
            sb.append(schemaId);
        }
        if (classSuffix != null) {
            sb.append("_");
            sb.append(classSuffix);
        }
        return sb.toString();
    }

    private Path writeProto(ProtobufSchema protobufSchema, String suffixedProtoClassName) {

        ProtobufSchema withJavaOuterClassName =
                setProtoOptions(protobufSchema, suffixedProtoClassName);
        String schema = withJavaOuterClassName.rawSchema().toSchema();
        String schemaFileName = suffixedProtoClassName + ".proto";
        Path schemaFilePath = protosDir.resolve(schemaFileName);
        try {
            java.io.FileWriter myWriter = new java.io.FileWriter(schemaFilePath.toFile());
            myWriter.write(schema);
            myWriter.close();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to write schema to " + schemaFilePath, e);
        }
        return schemaFilePath;
    }

    private ProtobufSchema setProtoOptions(ProtobufSchema protobufSchema, String className) {
        Descriptors.FileDescriptor originalDescriptor = protobufSchema.toDescriptor().getFile();

        String protoPackageName = protobufSchema.rawSchema().getPackageName();
        if (protoPackageName == null || protoPackageName.isEmpty()) {
            throw new IllegalArgumentException(
                    "Proto package name was null or empty in "
                            + protobufSchema.rawSchema().toSchema());
        }

        DescriptorProtos.FileOptions newOptions =
                DescriptorProtos.FileOptions.newBuilder()
                        .mergeFrom(originalDescriptor.getOptions())
                        // Without this, the generated class will be prefixed with the package name
                        // e.g. SomePackageNameClassName
                        .setJavaOuterClassname(className)
                        // For deterministic compilation, we will always set java package to proto
                        // package
                        .setJavaPackage(protoPackageName)
                        // Don't split the generated class into multiple files
                        .setJavaMultipleFiles(false)
                        .build();
        DescriptorProtos.FileDescriptorProto.Builder newProtoBuilder =
                originalDescriptor.toProto().toBuilder().setOptions(newOptions);

        DescriptorProtos.FileDescriptorProto newProto =
                maybeAddConfluentImports(newProtoBuilder, originalDescriptor.getDependencies());
        try {
            return new ProtobufSchema(
                    Descriptors.FileDescriptor.buildFrom(
                            newProto,
                            originalDescriptor
                                    .getDependencies()
                                    .toArray(new Descriptors.FileDescriptor[0])));
        } catch (Exception e) {
            throw new RuntimeException("Failed to set proto options.", e);
        }
    }

    // Debezium schemas sometimes contain references to confluent message types
    // but don't contain the necessary import statement. This can lead to
    // protoc errors. We will add the necessary imports if they are missing.
    private DescriptorProtos.FileDescriptorProto maybeAddConfluentImports(
            DescriptorProtos.FileDescriptorProto.Builder newProtoBuilder,
            List<Descriptors.FileDescriptor> originalDependencies) {
        List<String> originalDependencyNames =
                originalDependencies.stream()
                        .map(
                                dep -> {
                                    if (dep.getPackage().isEmpty() || dep.getName().contains("/")) {
                                        return dep.getName();
                                    }
                                    return String.format("%s/%s", dep.getPackage(), dep.getName());
                                })
                        .collect(Collectors.toList());

        if (!originalDependencyNames.contains(META_PROTO)
                && !originalDependencies.contains(MetaProto.getDescriptor())) {
            newProtoBuilder.addDependency(META_PROTO);
        }
        if (!originalDependencyNames.contains(DECIMAL_PROTO)
                && !originalDependencies.contains(DecimalProto.getDescriptor())) {
            newProtoBuilder.addDependency(DECIMAL_PROTO);
        }

        return newProtoBuilder.build();
    }

    private String getClassNameFromProto(ProtobufSchema protobufSchema) {
        String[] classNameParts = protobufSchema.name().split("\\.");
        return classNameParts[classNameParts.length - 1];
    }

    private void copyIncludedProto(String includedProto) {
        InputStream protoFileStream = ProtoCompiler.class.getResourceAsStream(includedProto);
        if (protoFileStream == null) {
            throw new IllegalArgumentException("Could not find the proto file: " + includedProto);
        }
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(protoFileStream, StandardCharsets.UTF_8));
        String protoFileContents = reader.lines().collect(Collectors.joining("\n"));

        // Create any necessary subdirectory
        String[] resourceNameParts = includedProto.substring(1).split("/");
        for (int i = 0; i < resourceNameParts.length - 1; i++) {
            String childDirName = resourceNameParts[i];
            Path currentParentDir =
                    parentDir.resolve(
                            String.join("/", Arrays.copyOfRange(resourceNameParts, 0, i)));
            createChildDir(currentParentDir, childDirName);
        }

        Path protoFilePath = parentDir.resolve(includedProto.substring(1));
        try {
            Files.write(protoFilePath, protoFileContents.getBytes());
        } catch (Exception e) {
            throw new RuntimeException("Failed to write proto file to " + protoFilePath, e);
        }
    }

    private static Path createParentDir() {
        try {
            return Files.createTempDirectory("protoc-");
        } catch (Exception e) {
            throw new RuntimeException("Could not create temporary directory", e);
        }
    }

    private static Path createChildDir(Path parentDir, String name) {
        if (!Files.exists(parentDir)) {
            throw new IllegalArgumentException("Parent directory does not exist");
        }

        Path childPath = parentDir.resolve(name);
        if (Files.exists(childPath)) {
            return childPath;
        }

        try {
            return Files.createDirectory(parentDir.resolve(name));
        } catch (Exception e) {
            throw new RuntimeException("Could not create child directory", e);
        }
    }

    private static String generateClassSuffix() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
