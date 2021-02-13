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

package org.apache.flink.client.program;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.security.FlinkSecurityManager;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.JarUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import static org.apache.flink.client.program.PackagedProgramUtils.isPython;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class encapsulates represents a program, packaged in a jar file. It supplies functionality
 * to extract nested libraries, search for the program entry point, and extract a program plan.
 */
public class PackagedProgram implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PackagedProgram.class);

    /**
     * Property name of the entry in JAR manifest file that describes the Flink specific entry
     * point.
     */
    public static final String MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS = "program-class";

    /**
     * Property name of the entry in JAR manifest file that describes the class with the main
     * method.
     */
    public static final String MANIFEST_ATTRIBUTE_MAIN_CLASS = "Main-Class";

    // --------------------------------------------------------------------------------------------

    private final URL jarFile;

    private final String[] args;

    private final Class<?> mainClass;

    private final List<File> extractedTempLibraries;

    private final List<URL> classpaths;

    private final URLClassLoader userCodeClassLoader;

    private final SavepointRestoreSettings savepointSettings;

    /** Flag indicating whether the job is a Python job. */
    private final boolean isPython;

    /**
     * Creates an instance that wraps the plan defined in the jar file using the given arguments.
     * For generating the plan the class defined in the className parameter is used.
     *
     * @param jarFile The jar file which contains the plan.
     * @param classpaths Additional classpath URLs needed by the Program.
     * @param entryPointClassName Name of the class which generates the plan. Overrides the class
     *     defined in the jar file manifest.
     * @param configuration Flink configuration which affects the classloading policy of the Program
     *     execution.
     * @param args Optional. The arguments used to create the pact plan, depend on implementation of
     *     the pact plan. See getDescription().
     * @throws ProgramInvocationException This invocation is thrown if the Program can't be properly
     *     loaded. Causes may be a missing / wrong class or manifest files.
     */
    private PackagedProgram(
            @Nullable File jarFile,
            List<URL> classpaths,
            @Nullable String entryPointClassName,
            Configuration configuration,
            SavepointRestoreSettings savepointRestoreSettings,
            String... args)
            throws ProgramInvocationException {
        this.classpaths = checkNotNull(classpaths);
        this.savepointSettings = checkNotNull(savepointRestoreSettings);
        this.args = checkNotNull(args);

        checkArgument(
                jarFile != null || entryPointClassName != null,
                "Either the jarFile or the entryPointClassName needs to be non-null.");

        // whether the job is a Python job.
        this.isPython = isPython(entryPointClassName);

        // load the jar file if exists
        this.jarFile = loadJarFile(jarFile);

        assert this.jarFile != null || entryPointClassName != null;

        // now that we have an entry point, we can extract the nested jar files (if any)
        this.extractedTempLibraries =
                this.jarFile == null
                        ? Collections.emptyList()
                        : extractContainedLibraries(this.jarFile);
        this.userCodeClassLoader =
                ClientUtils.buildUserCodeClassLoader(
                        getJobJarAndDependencies(),
                        classpaths,
                        getClass().getClassLoader(),
                        configuration);

        // load the entry point class
        this.mainClass =
                loadMainClass(
                        // if no entryPointClassName name was given, we try and look one up through
                        // the manifest
                        entryPointClassName != null
                                ? entryPointClassName
                                : getEntryPointClassNameFromJar(this.jarFile),
                        userCodeClassLoader);

        if (!hasMainMethod(mainClass)) {
            throw new ProgramInvocationException(
                    "The given program class does not have a main(String[]) method.");
        }
    }

    public SavepointRestoreSettings getSavepointSettings() {
        return savepointSettings;
    }

    public String[] getArguments() {
        return this.args;
    }

    public String getMainClassName() {
        return this.mainClass.getName();
    }

    /**
     * Returns the description provided by the Program class. This may contain a description of the
     * plan itself and its arguments.
     *
     * @return The description of the PactProgram's input parameters.
     * @throws ProgramInvocationException This invocation is thrown if the Program can't be properly
     *     loaded. Causes may be a missing / wrong class or manifest files.
     */
    @Nullable
    public String getDescription() throws ProgramInvocationException {
        if (ProgramDescription.class.isAssignableFrom(this.mainClass)) {

            ProgramDescription descr;
            try {
                descr =
                        InstantiationUtil.instantiate(
                                this.mainClass.asSubclass(ProgramDescription.class),
                                ProgramDescription.class);
            } catch (Throwable t) {
                return null;
            }

            try {
                return descr.getDescription();
            } catch (Throwable t) {
                throw new ProgramInvocationException(
                        "Error while getting the program description"
                                + (t.getMessage() == null ? "." : ": " + t.getMessage()),
                        t);
            }

        } else {
            return null;
        }
    }

    /**
     * This method assumes that the context environment is prepared, or the execution will be a
     * local execution by default.
     */
    public void invokeInteractiveModeForExecution() throws ProgramInvocationException {
        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            callMainMethod(mainClass, args);
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    /**
     * Returns the classpaths that are required by the program.
     *
     * @return List of {@link java.net.URL}s.
     */
    public List<URL> getClasspaths() {
        return this.classpaths;
    }

    /**
     * Gets the {@link java.lang.ClassLoader} that must be used to load user code classes.
     *
     * @return The user code ClassLoader.
     */
    public ClassLoader getUserCodeClassLoader() {
        return this.userCodeClassLoader;
    }

    /** Returns all provided libraries needed to run the program. */
    public List<URL> getJobJarAndDependencies() {
        List<URL> libs = new ArrayList<URL>(extractedTempLibraries.size() + 1);

        if (jarFile != null) {
            libs.add(jarFile);
        }
        for (File tmpLib : extractedTempLibraries) {
            try {
                libs.add(tmpLib.getAbsoluteFile().toURI().toURL());
            } catch (MalformedURLException e) {
                throw new RuntimeException("URL is invalid. This should not happen.", e);
            }
        }

        if (isPython) {
            libs.add(PackagedProgramUtils.getPythonJar());
        }

        return libs;
    }

    /** Returns all provided libraries needed to run the program. */
    public static List<URL> getJobJarAndDependencies(
            File jarFile, @Nullable String entryPointClassName) throws ProgramInvocationException {
        URL jarFileUrl = loadJarFile(jarFile);

        List<File> extractedTempLibraries =
                jarFileUrl == null
                        ? Collections.emptyList()
                        : extractContainedLibraries(jarFileUrl);

        List<URL> libs = new ArrayList<URL>(extractedTempLibraries.size() + 1);

        if (jarFileUrl != null) {
            libs.add(jarFileUrl);
        }
        for (File tmpLib : extractedTempLibraries) {
            try {
                libs.add(tmpLib.getAbsoluteFile().toURI().toURL());
            } catch (MalformedURLException e) {
                throw new RuntimeException("URL is invalid. This should not happen.", e);
            }
        }

        if (isPython(entryPointClassName)) {
            libs.add(PackagedProgramUtils.getPythonJar());
        }

        return libs;
    }

    /** Deletes all temporary files created for contained packaged libraries. */
    private void deleteExtractedLibraries() {
        deleteExtractedLibraries(this.extractedTempLibraries);
        this.extractedTempLibraries.clear();
    }

    private static boolean hasMainMethod(Class<?> entryClass) {
        Method mainMethod;
        try {
            mainMethod = entryClass.getMethod("main", String[].class);
        } catch (NoSuchMethodException e) {
            return false;
        } catch (Throwable t) {
            throw new RuntimeException(
                    "Could not look up the main(String[]) method from the class "
                            + entryClass.getName()
                            + ": "
                            + t.getMessage(),
                    t);
        }

        return Modifier.isStatic(mainMethod.getModifiers())
                && Modifier.isPublic(mainMethod.getModifiers());
    }

    private static void callMainMethod(Class<?> entryClass, String[] args)
            throws ProgramInvocationException {
        Method mainMethod;
        if (!Modifier.isPublic(entryClass.getModifiers())) {
            throw new ProgramInvocationException(
                    "The class " + entryClass.getName() + " must be public.");
        }

        try {
            mainMethod = entryClass.getMethod("main", String[].class);
        } catch (NoSuchMethodException e) {
            throw new ProgramInvocationException(
                    "The class " + entryClass.getName() + " has no main(String[]) method.");
        } catch (Throwable t) {
            throw new ProgramInvocationException(
                    "Could not look up the main(String[]) method from the class "
                            + entryClass.getName()
                            + ": "
                            + t.getMessage(),
                    t);
        }

        if (!Modifier.isStatic(mainMethod.getModifiers())) {
            throw new ProgramInvocationException(
                    "The class " + entryClass.getName() + " declares a non-static main method.");
        }
        if (!Modifier.isPublic(mainMethod.getModifiers())) {
            throw new ProgramInvocationException(
                    "The class " + entryClass.getName() + " declares a non-public main method.");
        }

        try {
            mainMethod.invoke(null, (Object) args);
        } catch (IllegalArgumentException e) {
            throw new ProgramInvocationException(
                    "Could not invoke the main method, arguments are not matching.", e);
        } catch (IllegalAccessException e) {
            throw new ProgramInvocationException(
                    "Access to the main method was denied: " + e.getMessage(), e);
        } catch (InvocationTargetException e) {
            Throwable exceptionInMethod = e.getTargetException();
            if (exceptionInMethod instanceof Error) {
                throw (Error) exceptionInMethod;
            } else if (exceptionInMethod instanceof ProgramParametrizationException) {
                throw (ProgramParametrizationException) exceptionInMethod;
            } else if (exceptionInMethod instanceof ProgramInvocationException) {
                throw (ProgramInvocationException) exceptionInMethod;
            } else {
                throw new ProgramInvocationException(
                        "The main method caused an error: " + exceptionInMethod.getMessage(),
                        exceptionInMethod);
            }
        } catch (Throwable t) {
            throw new ProgramInvocationException(
                    "An error occurred while invoking the program's main method: " + t.getMessage(),
                    t);
        }
    }

    private static String getEntryPointClassNameFromJar(URL jarFile)
            throws ProgramInvocationException {
        JarFile jar;
        Manifest manifest;
        String className;

        // Open jar file
        try {
            jar = new JarFile(new File(jarFile.toURI()));
        } catch (URISyntaxException use) {
            throw new ProgramInvocationException(
                    "Invalid file path '" + jarFile.getPath() + "'", use);
        } catch (IOException ioex) {
            throw new ProgramInvocationException(
                    "Error while opening jar file '"
                            + jarFile.getPath()
                            + "'. "
                            + ioex.getMessage(),
                    ioex);
        }

        // jar file must be closed at the end
        try {
            // Read from jar manifest
            try {
                manifest = jar.getManifest();
            } catch (IOException ioex) {
                throw new ProgramInvocationException(
                        "The Manifest in the jar file could not be accessed '"
                                + jarFile.getPath()
                                + "'. "
                                + ioex.getMessage(),
                        ioex);
            }

            if (manifest == null) {
                throw new ProgramInvocationException(
                        "No manifest found in jar file '"
                                + jarFile.getPath()
                                + "'. The manifest is need to point to the program's main class.");
            }

            Attributes attributes = manifest.getMainAttributes();

            // check for a "program-class" entry first
            className = attributes.getValue(PackagedProgram.MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS);
            if (className != null) {
                return className;
            }

            // check for a main class
            className = attributes.getValue(PackagedProgram.MANIFEST_ATTRIBUTE_MAIN_CLASS);
            if (className != null) {
                return className;
            } else {
                throw new ProgramInvocationException(
                        "Neither a '"
                                + MANIFEST_ATTRIBUTE_MAIN_CLASS
                                + "', nor a '"
                                + MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS
                                + "' entry was found in the jar file.");
            }
        } finally {
            try {
                jar.close();
            } catch (Throwable t) {
                throw new ProgramInvocationException(
                        "Could not close the JAR file: " + t.getMessage(), t);
            }
        }
    }

    @Nullable
    private static URL loadJarFile(File jar) throws ProgramInvocationException {
        if (jar != null) {
            URL jarFileUrl;

            try {
                jarFileUrl = jar.getAbsoluteFile().toURI().toURL();
            } catch (MalformedURLException e1) {
                throw new IllegalArgumentException("The jar file path is invalid.");
            }

            checkJarFile(jarFileUrl);

            return jarFileUrl;
        } else {
            return null;
        }
    }

    private static Class<?> loadMainClass(String className, ClassLoader cl)
            throws ProgramInvocationException {
        ClassLoader contextCl = null;
        try {
            contextCl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(cl);
            return Class.forName(className, false, cl);
        } catch (ClassNotFoundException e) {
            throw new ProgramInvocationException(
                    "The program's entry point class '"
                            + className
                            + "' was not found in the jar file.",
                    e);
        } catch (ExceptionInInitializerError e) {
            throw new ProgramInvocationException(
                    "The program's entry point class '"
                            + className
                            + "' threw an error during initialization.",
                    e);
        } catch (LinkageError e) {
            throw new ProgramInvocationException(
                    "The program's entry point class '"
                            + className
                            + "' could not be loaded due to a linkage failure.",
                    e);
        } catch (Throwable t) {
            throw new ProgramInvocationException(
                    "The program's entry point class '"
                            + className
                            + "' caused an exception during initialization: "
                            + t.getMessage(),
                    t);
        } finally {
            if (contextCl != null) {
                Thread.currentThread().setContextClassLoader(contextCl);
            }
        }
    }

    /**
     * Takes all JAR files that are contained in this program's JAR file and extracts them to the
     * system's temp directory.
     *
     * @return The file names of the extracted temporary files.
     * @throws ProgramInvocationException Thrown, if the extraction process failed.
     */
    public static List<File> extractContainedLibraries(URL jarFile)
            throws ProgramInvocationException {
        try (final JarFile jar = new JarFile(new File(jarFile.toURI()))) {

            final List<JarEntry> containedJarFileEntries = getContainedJarEntries(jar);
            if (containedJarFileEntries.isEmpty()) {
                return Collections.emptyList();
            }

            final List<File> extractedTempLibraries =
                    new ArrayList<>(containedJarFileEntries.size());
            boolean incomplete = true;

            try {
                final Random rnd = new Random();
                final byte[] buffer = new byte[4096];

                for (final JarEntry entry : containedJarFileEntries) {
                    // '/' as in case of zip, jar
                    // java.util.zip.ZipEntry#isDirectory always looks only for '/' not for
                    // File.separator
                    final String name = entry.getName().replace('/', '_');
                    final File tempFile = copyLibToTempFile(name, rnd, jar, entry, buffer);
                    extractedTempLibraries.add(tempFile);
                }

                incomplete = false;
            } finally {
                if (incomplete) {
                    deleteExtractedLibraries(extractedTempLibraries);
                }
            }

            return extractedTempLibraries;
        } catch (Throwable t) {
            throw new ProgramInvocationException(
                    "Unknown I/O error while extracting contained jar files.", t);
        }
    }

    private static File copyLibToTempFile(
            String name, Random rnd, JarFile jar, JarEntry input, byte[] buffer)
            throws ProgramInvocationException {
        final File output = createTempFile(rnd, input, name);
        try (final OutputStream out = new FileOutputStream(output);
                final InputStream in = new BufferedInputStream(jar.getInputStream(input))) {
            int numRead = 0;
            while ((numRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, numRead);
            }
            return output;
        } catch (IOException e) {
            throw new ProgramInvocationException(
                    "An I/O error occurred while extracting nested library '"
                            + input.getName()
                            + "' to temporary file '"
                            + output.getAbsolutePath()
                            + "'.");
        }
    }

    private static File createTempFile(Random rnd, JarEntry entry, String name)
            throws ProgramInvocationException {
        try {
            final File tempFile = File.createTempFile(rnd.nextInt(Integer.MAX_VALUE) + "_", name);
            tempFile.deleteOnExit();
            return tempFile;
        } catch (IOException e) {
            throw new ProgramInvocationException(
                    "An I/O error occurred while creating temporary file to extract nested library '"
                            + entry.getName()
                            + "'.",
                    e);
        }
    }

    private static List<JarEntry> getContainedJarEntries(JarFile jar) {
        return jar.stream()
                .filter(
                        jarEntry -> {
                            final String name = jarEntry.getName();
                            return name.length() > 8
                                    && name.startsWith("lib/")
                                    && name.endsWith(".jar");
                        })
                .collect(Collectors.toList());
    }

    private static void deleteExtractedLibraries(List<File> tempLibraries) {
        for (File f : tempLibraries) {
            f.delete();
        }
    }

    private static void checkJarFile(URL jarfile) throws ProgramInvocationException {
        try {
            JarUtils.checkJarFile(jarfile);
        } catch (IOException e) {
            throw new ProgramInvocationException(e.getMessage(), e);
        } catch (Throwable t) {
            throw new ProgramInvocationException(
                    "Cannot access jar file"
                            + (t.getMessage() == null ? "." : ": " + t.getMessage()),
                    t);
        }
    }

    @Override
    public void close() {
        try {
            userCodeClassLoader.close();
        } catch (IOException e) {
            LOG.debug("Error while closing user-code classloader.", e);
        }
        try {
            deleteExtractedLibraries();
        } catch (Exception e) {
            LOG.debug("Error while deleting jars extracted from user-jar.", e);
        }
    }

    /** A Builder For {@link PackagedProgram}. */
    public static class Builder {

        @Nullable private File jarFile;

        @Nullable private String entryPointClassName;

        private String[] args = new String[0];

        private List<URL> userClassPaths = Collections.emptyList();

        private Configuration configuration = new Configuration();

        private SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();

        public Builder setJarFile(@Nullable File jarFile) {
            this.jarFile = jarFile;
            return this;
        }

        public Builder setUserClassPaths(List<URL> userClassPaths) {
            this.userClassPaths = userClassPaths;
            return this;
        }

        public Builder setEntryPointClassName(@Nullable String entryPointClassName) {
            this.entryPointClassName = entryPointClassName;
            return this;
        }

        public Builder setArguments(String... args) {
            this.args = args;
            return this;
        }

        public Builder setConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public Builder setSavepointRestoreSettings(
                SavepointRestoreSettings savepointRestoreSettings) {
            this.savepointRestoreSettings = savepointRestoreSettings;
            return this;
        }

        public PackagedProgram build() throws ProgramInvocationException {
            if (jarFile == null && entryPointClassName == null) {
                throw new IllegalArgumentException(
                        "The jarFile and entryPointClassName can not be null at the same time.");
            }
            return new PackagedProgram(
                    jarFile,
                    userClassPaths,
                    entryPointClassName,
                    configuration,
                    savepointRestoreSettings,
                    args);
        }

        private Builder() {}
    }

    public static Builder newBuilder() {
        return new Builder();
    }
}
