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

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/** Utility class to create instances from class objects and checking failure reasons. */
@Internal
public final class InstantiationUtil {

    private static final Logger LOG = LoggerFactory.getLogger(InstantiationUtil.class);

    /** A custom ObjectInputStream that can load classes using a specific ClassLoader. */
    public static class ClassLoaderObjectInputStream extends ObjectInputStream {

        protected final ClassLoader classLoader;

        public ClassLoaderObjectInputStream(InputStream in, ClassLoader classLoader)
                throws IOException {
            super(in);
            this.classLoader = classLoader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
            if (classLoader != null) {
                String name = desc.getName();
                try {
                    return Class.forName(name, false, classLoader);
                } catch (ClassNotFoundException ex) {
                    // check if class is a primitive class
                    Class<?> cl = primitiveClasses.get(name);
                    if (cl != null) {
                        // return primitive class
                        return cl;
                    } else {
                        // throw ClassNotFoundException
                        throw ex;
                    }
                }
            }

            return super.resolveClass(desc);
        }

        @Override
        protected Class<?> resolveProxyClass(String[] interfaces)
                throws IOException, ClassNotFoundException {
            if (classLoader != null) {
                ClassLoader nonPublicLoader = null;
                boolean hasNonPublicInterface = false;

                // define proxy in class loader of non-public interface(s), if any
                Class<?>[] classObjs = new Class<?>[interfaces.length];
                for (int i = 0; i < interfaces.length; i++) {
                    Class<?> cl = Class.forName(interfaces[i], false, classLoader);
                    if ((cl.getModifiers() & Modifier.PUBLIC) == 0) {
                        if (hasNonPublicInterface) {
                            if (nonPublicLoader != cl.getClassLoader()) {
                                throw new IllegalAccessError(
                                        "conflicting non-public interface class loaders");
                            }
                        } else {
                            nonPublicLoader = cl.getClassLoader();
                            hasNonPublicInterface = true;
                        }
                    }
                    classObjs[i] = cl;
                }
                try {
                    return Proxy.getProxyClass(
                            hasNonPublicInterface ? nonPublicLoader : classLoader, classObjs);
                } catch (IllegalArgumentException e) {
                    throw new ClassNotFoundException(null, e);
                }
            }

            return super.resolveProxyClass(interfaces);
        }

        // ------------------------------------------------

        private static final HashMap<String, Class<?>> primitiveClasses =
                CollectionUtil.newHashMapWithExpectedSize(9);

        static {
            primitiveClasses.put("boolean", boolean.class);
            primitiveClasses.put("byte", byte.class);
            primitiveClasses.put("char", char.class);
            primitiveClasses.put("short", short.class);
            primitiveClasses.put("int", int.class);
            primitiveClasses.put("long", long.class);
            primitiveClasses.put("float", float.class);
            primitiveClasses.put("double", double.class);
            primitiveClasses.put("void", void.class);
        }
    }

    /**
     * Workaround for bugs like e.g. FLINK-36318 where we serialize a class into a snapshot and then
     * its serialVersionUID is changed in an uncontrolled way. This lets us deserialize the old
     * snapshot assuming the binary representation of the faulty class has not changed.
     */
    private static final class VersionMismatchHandler {

        private final Map<String, Map<Long, List<Long>>> supportedSerialVersionUidsPerClass =
                new HashMap<>();

        void addVersionsMatch(
                String className, long localVersionUID, List<Long> streamVersionUIDs) {
            supportedSerialVersionUidsPerClass
                    .computeIfAbsent(className, k -> new HashMap<>())
                    .put(localVersionUID, streamVersionUIDs);
        }

        /**
         * Checks if the local version of the given class can safely deserialize the class of a
         * different version from the object stream.
         */
        boolean shouldTolerateSerialVersionMismatch(
                String className, long localVersionUID, long streamVersionUID) {
            return supportedSerialVersionUidsPerClass
                    .getOrDefault(className, Collections.emptyMap())
                    .getOrDefault(localVersionUID, Collections.emptyList())
                    .contains(streamVersionUID);
        }

        /**
         * Checks if there are any rules for the given class. This lets us decide early if we need
         * to look up the local class.
         */
        boolean haveRulesForClass(String className) {
            return supportedSerialVersionUidsPerClass.containsKey(className);
        }
    }

    private static final VersionMismatchHandler versionMismatchHandler =
            new VersionMismatchHandler();

    static {
        // See FLINK-36318
        versionMismatchHandler.addVersionsMatch(
                "org.apache.flink.table.runtime.typeutils.MapDataSerializer",
                4073842523628732956L,
                Collections.singletonList(2533002123505507000L));
    }

    /**
     * An {@link ObjectInputStream} that ignores certain serialVersionUID mismatches. This is a
     * workaround for uncontrolled serialVersionUIDs changes.
     */
    public static class FailureTolerantObjectInputStream
            extends InstantiationUtil.ClassLoaderObjectInputStream {

        public FailureTolerantObjectInputStream(InputStream in, ClassLoader cl) throws IOException {
            super(in, cl);
        }

        @Override
        protected ObjectStreamClass readClassDescriptor()
                throws IOException, ClassNotFoundException {
            ObjectStreamClass streamClassDescriptor = super.readClassDescriptor();

            final Class localClass = resolveClass(streamClassDescriptor);
            final String name = localClass.getName();
            if (versionMismatchHandler.haveRulesForClass(name)) {
                final ObjectStreamClass localClassDescriptor = ObjectStreamClass.lookup(localClass);
                if (localClassDescriptor != null
                        && localClassDescriptor.getSerialVersionUID()
                                != streamClassDescriptor.getSerialVersionUID()) {
                    if (versionMismatchHandler.shouldTolerateSerialVersionMismatch(
                            name,
                            localClassDescriptor.getSerialVersionUID(),
                            streamClassDescriptor.getSerialVersionUID())) {
                        LOG.warn(
                                "Ignoring serialVersionUID mismatch for class {}; was {}, now {}.",
                                streamClassDescriptor.getName(),
                                streamClassDescriptor.getSerialVersionUID(),
                                localClassDescriptor.getSerialVersionUID());

                        streamClassDescriptor = localClassDescriptor;
                    }
                }
            }

            return streamClassDescriptor;
        }
    }

    /**
     * Creates a new instance of the given class name and type using the provided {@link
     * ClassLoader}.
     *
     * @param className of the class to load
     * @param targetType type of the instantiated class
     * @param classLoader to use for loading the class
     * @param <T> type of the instantiated class
     * @return Instance of the given class name
     * @throws FlinkException if the class could not be found
     */
    public static <T> T instantiate(
            final String className, final Class<T> targetType, final ClassLoader classLoader)
            throws FlinkException {
        final Class<? extends T> clazz;
        try {
            clazz = Class.forName(className, false, classLoader).asSubclass(targetType);
        } catch (ClassNotFoundException e) {
            throw new FlinkException(
                    String.format(
                            "Could not instantiate class '%s' of type '%s'. Please make sure that this class is on your class path.",
                            className, targetType.getName()),
                    e);
        }

        return instantiate(clazz);
    }

    /**
     * Creates a new instance of the given class.
     *
     * @param <T> The generic type of the class.
     * @param clazz The class to instantiate.
     * @param castTo Optional parameter, specifying the class that the given class must be a
     *     subclass off. This argument is added to prevent class cast exceptions occurring later.
     * @return An instance of the given class.
     * @throws RuntimeException Thrown, if the class could not be instantiated. The exception
     *     contains a detailed message about the reason why the instantiation failed.
     */
    public static <T> T instantiate(Class<T> clazz, Class<? super T> castTo) {
        if (clazz == null) {
            throw new NullPointerException();
        }

        // check if the class is a subclass, if the check is required
        if (castTo != null && !castTo.isAssignableFrom(clazz)) {
            throw new RuntimeException(
                    "The class '"
                            + clazz.getName()
                            + "' is not a subclass of '"
                            + castTo.getName()
                            + "' as is required.");
        }

        return instantiate(clazz);
    }

    /**
     * Creates a new instance of the given class.
     *
     * @param <T> The generic type of the class.
     * @param clazz The class to instantiate.
     * @return An instance of the given class.
     * @throws RuntimeException Thrown, if the class could not be instantiated. The exception
     *     contains a detailed message about the reason why the instantiation failed.
     */
    public static <T> T instantiate(Class<T> clazz) {
        if (clazz == null) {
            throw new NullPointerException();
        }

        // try to instantiate the class
        try {
            return clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException iex) {
            // check for the common problem causes
            checkForInstantiation(clazz);

            // here we are, if non of the common causes was the problem. then the error was
            // most likely an exception in the constructor or field initialization
            throw new RuntimeException(
                    "Could not instantiate type '"
                            + clazz.getName()
                            + "' due to an unspecified exception: "
                            + iex.getMessage(),
                    iex);
        } catch (Throwable t) {
            String message = t.getMessage();
            throw new RuntimeException(
                    "Could not instantiate type '"
                            + clazz.getName()
                            + "' Most likely the constructor (or a member variable initialization) threw an exception"
                            + (message == null ? "." : ": " + message),
                    t);
        }
    }

    /**
     * Checks, whether the given class has a public nullary constructor.
     *
     * @param clazz The class to check.
     * @return True, if the class has a public nullary constructor, false if not.
     */
    public static boolean hasPublicNullaryConstructor(Class<?> clazz) {
        return Arrays.stream(clazz.getConstructors())
                .anyMatch(constructor -> constructor.getParameterCount() == 0);
    }

    /**
     * Checks, whether the given class is public.
     *
     * @param clazz The class to check.
     * @return True, if the class is public, false if not.
     */
    public static boolean isPublic(Class<?> clazz) {
        return Modifier.isPublic(clazz.getModifiers());
    }

    /**
     * Checks, whether the class is a proper class, i.e. not abstract or an interface, and not a
     * primitive type.
     *
     * @param clazz The class to check.
     * @return True, if the class is a proper class, false otherwise.
     */
    public static boolean isProperClass(Class<?> clazz) {
        int mods = clazz.getModifiers();
        return !(Modifier.isAbstract(mods)
                || Modifier.isInterface(mods)
                || Modifier.isNative(mods));
    }

    /**
     * Checks, whether the class is an inner class that is not statically accessible. That is
     * especially true for anonymous inner classes.
     *
     * @param clazz The class to check.
     * @return True, if the class is a non-statically accessible inner class.
     */
    public static boolean isNonStaticInnerClass(Class<?> clazz) {
        return clazz.getEnclosingClass() != null
                && (clazz.getDeclaringClass() == null || !Modifier.isStatic(clazz.getModifiers()));
    }

    /**
     * Performs a standard check whether the class can be instantiated by {@code
     * Class#newInstance()}.
     *
     * @param clazz The class to check.
     * @throws RuntimeException Thrown, if the class cannot be instantiated by {@code
     *     Class#newInstance()}.
     */
    public static void checkForInstantiation(Class<?> clazz) {
        final String errorMessage = checkForInstantiationError(clazz);

        if (errorMessage != null) {
            throw new RuntimeException(
                    "The class '" + clazz.getName() + "' is not instantiable: " + errorMessage);
        }
    }

    public static String checkForInstantiationError(Class<?> clazz) {
        if (!isPublic(clazz)) {
            return "The class is not public.";
        } else if (clazz.isArray()) {
            return "The class is an array. An array cannot be simply instantiated, as with a parameterless constructor.";
        } else if (!isProperClass(clazz)) {
            return "The class is not a proper class. It is either abstract, an interface, or a primitive type.";
        } else if (isNonStaticInnerClass(clazz)) {
            return "The class is an inner class, but not statically accessible.";
        } else if (!hasPublicNullaryConstructor(clazz)) {
            return "The class has no (implicit) public nullary constructor, i.e. a constructor without arguments.";
        } else {
            return null;
        }
    }

    @Nullable
    public static <T> T readObjectFromConfig(Configuration config, String key, ClassLoader cl)
            throws IOException, ClassNotFoundException {
        byte[] bytes = config.getBytes(key, null);
        if (bytes == null) {
            return null;
        }

        return deserializeObject(bytes, cl);
    }

    public static void writeObjectToConfig(Object o, Configuration config, String key)
            throws IOException {
        byte[] bytes = serializeObject(o);
        config.setBytes(key, bytes);
    }

    public static <T> byte[] serializeToByteArray(TypeSerializer<T> serializer, T record)
            throws IOException {
        if (record == null) {
            throw new NullPointerException("Record to serialize to byte array must not be null.");
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream(64);
        DataOutputViewStreamWrapper outputViewWrapper = new DataOutputViewStreamWrapper(bos);
        serializer.serialize(record, outputViewWrapper);
        return bos.toByteArray();
    }

    public static <T> T deserializeFromByteArray(TypeSerializer<T> serializer, byte[] buf)
            throws IOException {
        if (buf == null) {
            throw new NullPointerException("Byte array to deserialize from must not be null.");
        }

        DataInputViewStreamWrapper inputViewWrapper =
                new DataInputViewStreamWrapper(new ByteArrayInputStream(buf));
        return serializer.deserialize(inputViewWrapper);
    }

    public static <T> T deserializeFromByteArray(TypeSerializer<T> serializer, T reuse, byte[] buf)
            throws IOException {
        if (buf == null) {
            throw new NullPointerException("Byte array to deserialize from must not be null.");
        }

        DataInputViewStreamWrapper inputViewWrapper =
                new DataInputViewStreamWrapper(new ByteArrayInputStream(buf));
        return serializer.deserialize(reuse, inputViewWrapper);
    }

    public static <T> T deserializeObject(byte[] bytes, ClassLoader cl)
            throws IOException, ClassNotFoundException {
        return deserializeObject(new ByteArrayInputStream(bytes), cl);
    }

    public static <T> T deserializeObject(InputStream in, ClassLoader cl)
            throws IOException, ClassNotFoundException {
        return deserializeObject(in, cl, false);
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserializeObject(
            InputStream in, ClassLoader cl, boolean tolerateKnownVersionMismatch)
            throws IOException, ClassNotFoundException {

        final ClassLoader old = Thread.currentThread().getContextClassLoader();
        // not using resource try to avoid AutoClosable's close() on the given stream
        try {
            ObjectInputStream oois =
                    tolerateKnownVersionMismatch
                            ? new InstantiationUtil.FailureTolerantObjectInputStream(in, cl)
                            : new InstantiationUtil.ClassLoaderObjectInputStream(in, cl);
            Thread.currentThread().setContextClassLoader(cl);
            return (T) oois.readObject();
        } finally {
            Thread.currentThread().setContextClassLoader(old);
        }
    }

    public static <T> T decompressAndDeserializeObject(byte[] bytes, ClassLoader cl)
            throws IOException, ClassNotFoundException {
        return deserializeObject(new InflaterInputStream(new ByteArrayInputStream(bytes)), cl);
    }

    public static byte[] serializeObject(Object o) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
            oos.flush();
            return baos.toByteArray();
        }
    }

    public static void serializeObject(OutputStream out, Object o) throws IOException {
        ObjectOutputStream oos =
                out instanceof ObjectOutputStream
                        ? (ObjectOutputStream) out
                        : new ObjectOutputStream(out);
        oos.writeObject(o);
    }

    public static byte[] serializeObjectAndCompress(Object o) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DeflaterOutputStream dos = new DeflaterOutputStream(baos);
                ObjectOutputStream oos = new ObjectOutputStream(dos)) {
            oos.writeObject(o);
            oos.flush();
            dos.close();
            return baos.toByteArray();
        }
    }

    public static boolean isSerializable(Object o) {
        try {
            serializeObject(o);
        } catch (IOException e) {
            return false;
        }

        return true;
    }

    /**
     * Clones the given serializable object using Java serialization.
     *
     * @param obj Object to clone
     * @param <T> Type of the object to clone
     * @return The cloned object
     * @throws IOException Thrown if the serialization or deserialization process fails.
     * @throws ClassNotFoundException Thrown if any of the classes referenced by the object cannot
     *     be resolved during deserialization.
     */
    public static <T extends Serializable> T clone(T obj)
            throws IOException, ClassNotFoundException {
        if (obj == null) {
            return null;
        } else {
            return clone(obj, obj.getClass().getClassLoader());
        }
    }

    /**
     * Clones the given serializable object using Java serialization, using the given classloader to
     * resolve the cloned classes.
     *
     * @param obj Object to clone
     * @param classLoader The classloader to resolve the classes during deserialization.
     * @param <T> Type of the object to clone
     * @return Cloned object
     * @throws IOException Thrown if the serialization or deserialization process fails.
     * @throws ClassNotFoundException Thrown if any of the classes referenced by the object cannot
     *     be resolved during deserialization.
     */
    public static <T extends Serializable> T clone(T obj, ClassLoader classLoader)
            throws IOException, ClassNotFoundException {
        if (obj == null) {
            return null;
        } else {
            final byte[] serializedObject = serializeObject(obj);
            return deserializeObject(serializedObject, classLoader);
        }
    }

    /**
     * Unchecked equivalent of {@link #clone(Serializable)}.
     *
     * @param obj Object to clone
     * @param <T> Type of the object to clone
     * @return The cloned object
     */
    public static <T extends Serializable> T cloneUnchecked(T obj) {
        try {
            return clone(obj, obj.getClass().getClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(
                    String.format("Unable to clone instance of %s.", obj.getClass().getName()), e);
        }
    }

    /**
     * Clones the given writable using the {@link IOReadableWritable serialization}.
     *
     * @param original Object to clone
     * @param <T> Type of the object to clone
     * @return Cloned object
     * @throws IOException Thrown is the serialization fails.
     */
    public static <T extends IOReadableWritable> T createCopyWritable(T original)
            throws IOException {
        if (original == null) {
            return null;
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos)) {
            original.write(out);
        }

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        try (DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais)) {

            @SuppressWarnings("unchecked")
            T copy = (T) instantiate(original.getClass());
            copy.read(in);
            return copy;
        }
    }

    /**
     * Loads a class by name from the given input stream and reflectively instantiates it.
     *
     * <p>This method will use {@link DataInputView#readUTF()} to read the class name, and then
     * attempt to load the class from the given ClassLoader.
     *
     * @param in The stream to read the class name from.
     * @param cl The class loader to resolve the class.
     * @throws IOException Thrown, if the class name could not be read, the class could not be
     *     found.
     */
    public static <T> Class<T> resolveClassByName(DataInputView in, ClassLoader cl)
            throws IOException {
        return resolveClassByName(in, cl, Object.class);
    }

    /**
     * Loads a class by name from the given input stream and reflectively instantiates it.
     *
     * <p>This method will use {@link DataInputView#readUTF()} to read the class name, and then
     * attempt to load the class from the given ClassLoader.
     *
     * <p>The resolved class is checked to be equal to or a subtype of the given supertype class.
     *
     * @param in The stream to read the class name from.
     * @param cl The class loader to resolve the class.
     * @param supertype A class that the resolved class must extend.
     * @throws IOException Thrown, if the class name could not be read, the class could not be
     *     found, or the class is not a subtype of the given supertype class.
     */
    public static <T> Class<T> resolveClassByName(
            DataInputView in, ClassLoader cl, Class<? super T> supertype) throws IOException {

        final String className = in.readUTF();
        final Class<?> rawClazz;
        try {
            rawClazz = Class.forName(className, false, cl);
        } catch (ClassNotFoundException e) {
            String error = "Could not find class '" + className + "' in classpath.";
            if (className.contains("SerializerConfig")) {
                error +=
                        " TypeSerializerConfigSnapshot and it's subclasses are not supported since Flink 1.17."
                                + " If you are using built-in serializers, please first migrate to Flink 1.16."
                                + " If you are using custom serializers, please migrate them to"
                                + " TypeSerializerSnapshot using Flink 1.16.";
            }
            throw new IOException(error, e);
        }

        if (!supertype.isAssignableFrom(rawClazz)) {
            throw new IOException(
                    "The class " + className + " is not a subclass of " + supertype.getName());
        }

        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) rawClazz;
        return clazz;
    }

    // --------------------------------------------------------------------------------------------

    /** Private constructor to prevent instantiation. */
    private InstantiationUtil() {
        throw new RuntimeException();
    }
}
