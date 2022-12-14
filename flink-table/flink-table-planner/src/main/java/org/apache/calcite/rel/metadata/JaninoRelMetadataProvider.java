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
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.janino.RelMetadataHandlerGeneratorUtil;
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
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Copied to fix calcite issues. This class should be removed together with upgrade Janino to
 * 3.1.9+(https://issues.apache.org/jira/browse/FLINK-27995). FLINK modifications are at lines
 *
 * <ol>
 *   <li>Line 158 ~ 165
 * </ol>
 */
public class JaninoRelMetadataProvider implements RelMetadataProvider, MetadataHandlerProvider {
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

    private static <MH extends MetadataHandler<?>> MH generateCompileAndInstantiate(
            Class<MH> handlerClass, List<? extends MetadataHandler<?>> handlers) {

        final List<? extends MetadataHandler<?>> uniqueHandlers =
                handlers.stream().distinct().collect(Collectors.toList());
        RelMetadataHandlerGeneratorUtil.HandlerNameAndGeneratedCode handlerNameAndGeneratedCode =
                RelMetadataHandlerGeneratorUtil.generateHandler(handlerClass, uniqueHandlers);

        try {
            return compile(
                    handlerNameAndGeneratedCode.getHandlerName(),
                    handlerNameAndGeneratedCode.getGeneratedCode(),
                    handlerClass,
                    uniqueHandlers);
        } catch (CompileException | IOException e) {
            throw new RuntimeException(
                    "Error compiling:\n" + handlerNameAndGeneratedCode.getGeneratedCode(), e);
        }
    }

    static <MH extends MetadataHandler<?>> MH compile(
            String className,
            String generatedCode,
            Class<MH> handlerClass,
            List<? extends Object> argList)
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

        if (CalciteSystemProperty.DEBUG.value()) {
            // Add line numbers to the generated janino class
            compiler.setDebuggingInformation(true, true, true);
            System.out.println(generatedCode);
        }

        compiler.cook(generatedCode);
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

    @Override
    public synchronized <H extends MetadataHandler<?>> H revise(Class<H> handlerClass) {
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
     * action is probably to re-generate the handler class. Use {@link
     * MetadataHandlerProvider.NoHandler} instead.
     */
    @Deprecated
    public static class NoHandler extends MetadataHandlerProvider.NoHandler {
        public NoHandler(Class<? extends RelNode> relClass) {
            super(relClass);
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

    @SuppressWarnings("deprecation")
    @Override
    public <MH extends MetadataHandler<?>> MH handler(final Class<MH> handlerClass) {
        return handlerClass.cast(
                Proxy.newProxyInstance(
                        RelMetadataQuery.class.getClassLoader(),
                        new Class[] {handlerClass},
                        (proxy, method, args) -> {
                            final RelNode r =
                                    requireNonNull((RelNode) args[0], "(RelNode) args[0]");
                            throw new NoHandler(r.getClass());
                        }));
    }
}
