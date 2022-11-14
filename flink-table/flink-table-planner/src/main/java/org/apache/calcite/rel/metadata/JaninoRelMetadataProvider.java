/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.metadata;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.janino.DescriptiveCacheKey;
import org.apache.calcite.rel.metadata.janino.DispatchGenerator;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.codehaus.commons.compiler.ISimpleCompiler;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Copied to fix calcite issues. This class should be removed together with upgrade Janino to
 * 3.1.9+(https://issues.apache.org/jira/browse/FLINK-27995). FLINK modifications are at lines
 *
 * <ol>
 *   <li>Line 347 ~ 354
 * </ol>
 */
public class JaninoRelMetadataProvider implements RelMetadataProvider {
    private final RelMetadataProvider provider;

    // Constants and static fields

    public static final JaninoRelMetadataProvider DEFAULT =
            JaninoRelMetadataProvider.of(DefaultRelMetadataProvider.INSTANCE);

    /**
     * Cache of pre-generated handlers by provider and kind of metadata. For the cache to be
     * effective, providers should implement identity correctly.
     */
    private static final LoadingCache<Key, MetadataHandler<?>> HANDLERS =
            maxSize(
                            CacheBuilder.newBuilder(),
                            CalciteSystemProperty.METADATA_HANDLER_CACHE_MAXIMUM_SIZE.value())
                    .build(
                            CacheLoader.from(
                                    key ->
                                            generateCompileAndInstantiate(
                                                    key.handlerClass,
                                                    key.provider.handlers(key.handlerClass))));

    /** Private constructor; use {@link #of}. */
    private JaninoRelMetadataProvider(RelMetadataProvider provider) {
        this.provider = provider;
    }

    /**
     * Creates a JaninoRelMetadataProvider.
     *
     * @param provider Underlying provider
     */
    public static JaninoRelMetadataProvider of(RelMetadataProvider provider) {
        if (provider instanceof JaninoRelMetadataProvider) {
            return (JaninoRelMetadataProvider) provider;
        }
        return new JaninoRelMetadataProvider(provider);
    }

    // helper for initialization
    private static <K, V> CacheBuilder<K, V> maxSize(CacheBuilder<K, V> builder, int size) {
        if (size >= 0) {
            builder.maximumSize(size);
        }
        return builder;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        return obj == this
                || obj instanceof JaninoRelMetadataProvider
                        && ((JaninoRelMetadataProvider) obj).provider.equals(provider);
    }

    @Override
    public int hashCode() {
        return 109 + provider.hashCode();
    }

    @Deprecated // to be removed before 2.0
    @Override
    public <@Nullable M extends @Nullable Metadata> UnboundMetadata<M> apply(
            Class<? extends RelNode> relClass, Class<? extends M> metadataClass) {
        throw new UnsupportedOperationException();
    }

    @Deprecated // to be removed before 2.0
    @Override
    public <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers(MetadataDef<M> def) {
        return provider.handlers(def);
    }

    @Override
    public List<MetadataHandler<?>> handlers(Class<? extends MetadataHandler<?>> handlerClass) {
        return provider.handlers(handlerClass);
    }

    private static <MH extends MetadataHandler> MH generateCompileAndInstantiate(
            Class<MH> handlerClass, List<? extends MetadataHandler<?>> handlers) {
        final LinkedHashSet<? extends MetadataHandler<?>> handlerSet =
                new LinkedHashSet<>(handlers);
        final StringBuilder buff = new StringBuilder();
        final String name = "GeneratedMetadata_" + simpleNameForHandler(handlerClass);

        final Map<MetadataHandler<?>, String> handlerToName = new LinkedHashMap<>();
        for (MetadataHandler<?> provider : handlerSet) {
            if (!handlerToName.containsKey(provider)) {
                handlerToName.put(provider, "provider" + handlerToName.size());
            }
        }
        // Properties
        for (Map.Entry<MetadataHandler<?>, String> handlerAndName : handlerToName.entrySet()) {
            buff.append("  public final ")
                    .append(handlerAndName.getKey().getClass().getName())
                    .append(' ')
                    .append(handlerAndName.getValue())
                    .append(";\n");
        }
        // Constructor
        buff.append("  public ").append(name).append("(\n");
        for (Map.Entry<MetadataHandler<?>, String> handlerAndName : handlerToName.entrySet()) {
            buff.append("      ")
                    .append(handlerAndName.getKey().getClass().getName())
                    .append(' ')
                    .append(handlerAndName.getValue())
                    .append(",\n");
        }
        if (!handlerToName.isEmpty()) {
            // Remove trailing comma and new line
            buff.setLength(buff.length() - 2);
        }
        buff.append(") {\n");
        for (String handlerName : handlerToName.values()) {
            buff.append("    this.")
                    .append(handlerName)
                    .append(" = ")
                    .append(handlerName)
                    .append(";\n");
        }
        buff.append("  }\n");
        getDefMethod(buff, handlerToName.values().stream().findFirst().orElse(null));

        DispatchGenerator dispatchGenerator = new DispatchGenerator(handlerToName);
        for (Ord<Method> method : Ord.zip(handlerClass.getDeclaredMethods())) {
            cacheProperties(buff, method.e, method.i);
            generateCachedMethod(buff, method.e, method.i);
            dispatchGenerator.dispatchMethod(buff, method.e, handlerSet);
        }
        final List<Object> argList = new ArrayList<>(handlerToName.keySet());
        try {
            return compile(name, buff.toString(), handlerClass, argList);
        } catch (CompileException | IOException e) {
            throw new RuntimeException("Error compiling:\n" + buff, e);
        }
    }

    static void cacheProperties(StringBuilder buff, Method method, int methodIndex) {
        buff.append("  private final Object ");
        appendKeyName(buff, methodIndex);
        buff.append(" = new ")
                .append(DescriptiveCacheKey.class.getName())
                .append("(\"")
                .append(method.toString())
                .append("\");\n");
    }

    private static void appendKeyName(StringBuilder buff, int methodIndex) {
        buff.append("methodKey").append(methodIndex);
    }

    private static void getDefMethod(StringBuilder buff, @Nullable String handlerName) {
        buff.append("  public ").append(MetadataDef.class.getName()).append(" getDef() {\n");

        if (handlerName == null) {
            buff.append("    return null;");
        } else {
            buff.append("    return ").append(handlerName).append(".getDef();\n");
        }
        buff.append("  }\n");
    }

    private static void generateCachedMethod(StringBuilder buff, Method method, int methodIndex) {
        String delRelClass = DelegatingMetadataRel.class.getName();
        buff.append("  public ")
                .append(method.getReturnType().getName())
                .append(" ")
                .append(method.getName())
                .append("(\n")
                .append("      ")
                .append(RelNode.class.getName())
                .append(" r,\n")
                .append("      ")
                .append(RelMetadataQuery.class.getName())
                .append(" mq");
        paramList(buff, method)
                .append(") {\n")
                .append("    while (r instanceof ")
                .append(delRelClass)
                .append(") {\n")
                .append("      r = ((")
                .append(delRelClass)
                .append(") r).getMetadataDelegateRel();\n")
                .append("    }\n")
                .append("    final java.util.List key = ")
                .append(
                        (method.getParameterTypes().length < 4
                                        ? org.apache.calcite.runtime.FlatLists.class
                                        : ImmutableList.class)
                                .getName())
                .append(".of(");
        appendKeyName(buff, methodIndex);
        safeArgList(buff, method)
                .append(");\n")
                .append("    final Object v = mq.map.get(r, key);\n")
                .append("    if (v != null) {\n")
                .append("      if (v == ")
                .append(NullSentinel.class.getName())
                .append(".ACTIVE) {\n")
                .append("        throw new ")
                .append(CyclicMetadataException.class.getName())
                .append("();\n")
                .append("      }\n")
                .append("      if (v == ")
                .append(NullSentinel.class.getName())
                .append(".INSTANCE) {\n")
                .append("        return null;\n")
                .append("      }\n")
                .append("      return (")
                .append(method.getReturnType().getName())
                .append(") v;\n")
                .append("    }\n")
                .append("    mq.map.put(r, key,")
                .append(NullSentinel.class.getName())
                .append(".ACTIVE);\n")
                .append("    try {\n")
                .append("      final ")
                .append(method.getReturnType().getName())
                .append(" x = ")
                .append(method.getName())
                .append("_(r, mq");
        argList(buff, method)
                .append(");\n")
                .append("      mq.map.put(r, key, ")
                .append(NullSentinel.class.getName())
                .append(".mask(x));\n")
                .append("      return x;\n")
                .append("    } catch (")
                .append(Exception.class.getName())
                .append(" e) {\n")
                .append("      mq.map.row(r).clear();\n")
                .append("      throw e;\n")
                .append("    }\n")
                .append("  }\n")
                .append("\n");
    }

    private static String simpleNameForHandler(Class<? extends MetadataHandler> clazz) {
        String simpleName = clazz.getSimpleName();
        // Previously the pattern was to have a nested in class named Handler
        // So we need to add the parents class to get a unique name
        if (simpleName.equals("Handler")) {
            String[] parts = clazz.getName().split("\\.|\\$");
            return parts[parts.length - 2] + parts[parts.length - 1];
        } else {
            return simpleName;
        }
    }

    /** Returns e.g. ", ignoreNulls". */
    private static StringBuilder argList(StringBuilder buff, Method method) {
        Class<?>[] paramTypes = method.getParameterTypes();
        for (int i = 2; i < paramTypes.length; i++) {
            buff.append(", a").append(i - 2);
        }
        return buff;
    }

    /** Returns e.g. ", ignoreNulls". */
    private static StringBuilder safeArgList(StringBuilder buff, Method method) {
        Class<?>[] paramTypes = method.getParameterTypes();
        for (int i = 2; i < paramTypes.length; i++) {
            Class<?> t = paramTypes[i];
            if (Primitive.is(t)) {
                buff.append(", a").append(i - 2);
            } else {
                buff.append(", ")
                        .append(NullSentinel.class.getName())
                        .append(".mask(a")
                        .append(i - 2)
                        .append(")");
            }
        }
        return buff;
    }

    /** Returns e.g. ",\n boolean ignoreNulls". */
    private static StringBuilder paramList(StringBuilder buff, Method method) {
        Class<?>[] paramTypes = method.getParameterTypes();
        for (int i = 2; i < paramTypes.length; i++) {
            buff.append(",\n      ").append(paramTypes[i].getName()).append(" a").append(i - 2);
        }
        return buff;
    }

    static <MH extends MetadataHandler<?>> MH compile(
            String className, String classBody, Class<MH> handlerClass, List<Object> argList)
            throws CompileException, IOException {
        // FLINK MODIFICATION BEGIN
        final ICompilerFactory compilerFactory;
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to instantiate java compiler", e);
        }
        // FLINK MODIFICATION END

        final ISimpleCompiler compiler = compilerFactory.newSimpleCompiler();
        compiler.setParentClassLoader(JaninoRexCompiler.class.getClassLoader());

        final String s =
                "public final class "
                        + className
                        + " implements "
                        + handlerClass.getCanonicalName()
                        + " {\n"
                        + classBody
                        + "\n"
                        + "}";

        if (CalciteSystemProperty.DEBUG.value()) {
            // Add line numbers to the generated janino class
            compiler.setDebuggingInformation(true, true, true);
            System.out.println(s);
        }

        compiler.cook(s);
        final Constructor constructor;
        final Object o;
        try {
            constructor =
                    compiler.getClassLoader().loadClass(className).getDeclaredConstructors()[0];
            o = constructor.newInstance(argList.toArray());
        } catch (InstantiationException
                | IllegalAccessException
                | InvocationTargetException
                | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return handlerClass.cast(o);
    }

    synchronized <H extends MetadataHandler<?>> H revise(Class<H> handlerClass) {
        try {
            final Key key = new Key(handlerClass, provider);
            //noinspection unchecked
            return handlerClass.cast(HANDLERS.get(key));
        } catch (UncheckedExecutionException | ExecutionException e) {
            throw Util.throwAsRuntime(Util.causeOrSelf(e));
        }
    }

    /**
     * Registers some classes. Does not flush the providers, but next time we need to generate a
     * provider, it will handle all of these classes. So, calling this method reduces the number of
     * times we need to re-generate.
     */
    @Deprecated
    public void register(Iterable<Class<? extends RelNode>> classes) {}

    /**
     * Exception that indicates there there should be a handler for this class but there is not. The
     * action is probably to re-generate the handler class.
     */
    public static class NoHandler extends ControlFlowException {
        public final Class<? extends RelNode> relClass;

        public NoHandler(Class<? extends RelNode> relClass) {
            this.relClass = relClass;
        }
    }

    /** Key for the cache. */
    private static class Key {
        final Class<? extends MetadataHandler<? extends Metadata>> handlerClass;
        final RelMetadataProvider provider;

        private Key(
                Class<? extends MetadataHandler<?>> handlerClass, RelMetadataProvider provider) {
            this.handlerClass = handlerClass;
            this.provider = provider;
        }

        @Override
        public int hashCode() {
            return (handlerClass.hashCode() * 37 + provider.hashCode()) * 37;
        }

        @Override
        public boolean equals(@Nullable Object obj) {
            return this == obj
                    || obj instanceof Key
                            && ((Key) obj).handlerClass.equals(handlerClass)
                            && ((Key) obj).provider.equals(provider);
        }
    }
}
