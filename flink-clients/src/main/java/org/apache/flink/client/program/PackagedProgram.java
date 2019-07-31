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

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.dag.DataSinkNode;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * This class encapsulates represents a program, packaged in a jar file. It supplies
 * functionality to extract nested libraries, search for the program entry point, and extract
 * a program plan.
 */
public class PackagedProgram {

	/**
	 * Property name of the entry in JAR manifest file that describes the Flink specific entry point.
	 */
	public static final String MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS = "program-class";

	/**
	 * Property name of the entry in JAR manifest file that describes the class with the main method.
	 */
	public static final String MANIFEST_ATTRIBUTE_MAIN_CLASS = "Main-Class";

	// --------------------------------------------------------------------------------------------

	private final URL jarFile;

	private final String[] args;

	private final Program program;

	private final Class<?> mainClass;

	private final List<File> extractedTempLibraries;

	private final List<URL> classpaths;

	private ClassLoader userCodeClassLoader;

	private Plan plan;

	private SavepointRestoreSettings savepointSettings = SavepointRestoreSettings.none();

	/**
	 * Flag indicating whether the job is a Python job.
	 */
	private final boolean isPython;

	/**
	 * Creates an instance that wraps the plan defined in the jar file using the given
	 * argument.
	 *
	 * @param jarFile
	 *        The jar file which contains the plan and a Manifest which defines
	 *        the program-class
	 * @param args
	 *        Optional. The arguments used to create the pact plan, depend on
	 *        implementation of the pact plan. See getDescription().
	 * @throws ProgramInvocationException
	 *         This invocation is thrown if the Program can't be properly loaded. Causes
	 *         may be a missing / wrong class or manifest files.
	 */
	public PackagedProgram(File jarFile, String... args) throws ProgramInvocationException {
		this(jarFile, Collections.<URL>emptyList(), null, args);
	}

	/**
	 * Creates an instance that wraps the plan defined in the jar file using the given
	 * argument.
	 *
	 * @param jarFile
	 *        The jar file which contains the plan and a Manifest which defines
	 *        the program-class
	 * @param classpaths
	 *        Additional classpath URLs needed by the Program.
	 * @param args
	 *        Optional. The arguments used to create the pact plan, depend on
	 *        implementation of the pact plan. See getDescription().
	 * @throws ProgramInvocationException
	 *         This invocation is thrown if the Program can't be properly loaded. Causes
	 *         may be a missing / wrong class or manifest files.
	 */
	public PackagedProgram(File jarFile, List<URL> classpaths, String... args) throws ProgramInvocationException {
		this(jarFile, classpaths, null, args);
	}

	/**
	 * Creates an instance that wraps the plan defined in the jar file using the given
	 * arguments. For generating the plan the class defined in the className parameter
	 * is used.
	 *
	 * @param jarFile
	 *        The jar file which contains the plan.
	 * @param entryPointClassName
	 *        Name of the class which generates the plan. Overrides the class defined
	 *        in the jar file manifest
	 * @param args
	 *        Optional. The arguments used to create the pact plan, depend on
	 *        implementation of the pact plan. See getDescription().
	 * @throws ProgramInvocationException
	 *         This invocation is thrown if the Program can't be properly loaded. Causes
	 *         may be a missing / wrong class or manifest files.
	 */
	public PackagedProgram(File jarFile, @Nullable String entryPointClassName, String... args) throws ProgramInvocationException {
		this(jarFile, Collections.<URL>emptyList(), entryPointClassName, args);
	}

	/**
	 * Creates an instance that wraps the plan defined in the jar file using the given
	 * arguments. For generating the plan the class defined in the className parameter
	 * is used.
	 *
	 * @param jarFile
	 *        The jar file which contains the plan.
	 * @param classpaths
	 *        Additional classpath URLs needed by the Program.
	 * @param entryPointClassName
	 *        Name of the class which generates the plan. Overrides the class defined
	 *        in the jar file manifest
	 * @param args
	 *        Optional. The arguments used to create the pact plan, depend on
	 *        implementation of the pact plan. See getDescription().
	 * @throws ProgramInvocationException
	 *         This invocation is thrown if the Program can't be properly loaded. Causes
	 *         may be a missing / wrong class or manifest files.
	 */
	public PackagedProgram(File jarFile, List<URL> classpaths, @Nullable String entryPointClassName, String... args) throws ProgramInvocationException {
		// Whether the job is a Python job.
		isPython = entryPointClassName != null && (entryPointClassName.equals("org.apache.flink.client.python.PythonDriver")
			|| entryPointClassName.equals("org.apache.flink.client.python.PythonGatewayServer"));

		URL jarFileUrl = null;
		if (jarFile != null) {
			try {
				jarFileUrl = jarFile.getAbsoluteFile().toURI().toURL();
			} catch (MalformedURLException e1) {
				throw new IllegalArgumentException("The jar file path is invalid.");
			}

			checkJarFile(jarFileUrl);
		} else if (!isPython) {
			throw new IllegalArgumentException("The jar file must not be null.");
		}

		this.jarFile = jarFileUrl;
		this.args = args == null ? new String[0] : args;

		// if no entryPointClassName name was given, we try and look one up through the manifest
		if (entryPointClassName == null) {
			entryPointClassName = getEntryPointClassNameFromJar(jarFileUrl);
		}

		// now that we have an entry point, we can extract the nested jar files (if any)
		this.extractedTempLibraries = jarFileUrl == null ? Collections.emptyList() : extractContainedLibraries(jarFileUrl);
		this.classpaths = classpaths;
		this.userCodeClassLoader = JobWithJars.buildUserCodeClassLoader(getAllLibraries(), classpaths, getClass().getClassLoader());

		// load the entry point class
		this.mainClass = loadMainClass(entryPointClassName, userCodeClassLoader);

		// if the entry point is a program, instantiate the class and get the plan
		if (Program.class.isAssignableFrom(this.mainClass)) {
			Program prg = null;
			try {
				prg = InstantiationUtil.instantiate(this.mainClass.asSubclass(Program.class), Program.class);
			} catch (Exception e) {
				// validate that the class has a main method at least.
				// the main method possibly instantiates the program properly
				if (!hasMainMethod(mainClass)) {
					throw new ProgramInvocationException("The given program class implements the " +
							Program.class.getName() + " interface, but cannot be instantiated. " +
							"It also declares no main(String[]) method as alternative entry point", e);
				}
			} catch (Throwable t) {
				throw new ProgramInvocationException("Error while trying to instantiate program class.", t);
			}
			this.program = prg;
		} else if (hasMainMethod(mainClass)) {
			this.program = null;
		} else {
			throw new ProgramInvocationException("The given program class neither has a main(String[]) method, nor does it implement the " +
					Program.class.getName() + " interface.");
		}
	}

	public PackagedProgram(Class<?> entryPointClass, String... args) throws ProgramInvocationException {
		this.jarFile = null;
		this.args = args == null ? new String[0] : args;

		this.extractedTempLibraries = Collections.emptyList();
		this.classpaths = Collections.emptyList();
		this.userCodeClassLoader = entryPointClass.getClassLoader();

		// load the entry point class
		this.mainClass = entryPointClass;
		isPython = entryPointClass.getCanonicalName().equals("org.apache.flink.client.python.PythonDriver");

		// if the entry point is a program, instantiate the class and get the plan
		if (Program.class.isAssignableFrom(this.mainClass)) {
			Program prg = null;
			try {
				prg = InstantiationUtil.instantiate(this.mainClass.asSubclass(Program.class), Program.class);
			} catch (Exception e) {
				// validate that the class has a main method at least.
				// the main method possibly instantiates the program properly
				if (!hasMainMethod(mainClass)) {
					throw new ProgramInvocationException("The given program class implements the " +
							Program.class.getName() + " interface, but cannot be instantiated. " +
							"It also declares no main(String[]) method as alternative entry point", e);
				}
			} catch (Throwable t) {
				throw new ProgramInvocationException("Error while trying to instantiate program class.", t);
			}
			this.program = prg;
		} else if (hasMainMethod(mainClass)) {
			this.program = null;
		} else {
			throw new ProgramInvocationException("The given program class neither has a main(String[]) method, nor does it implement the " +
					Program.class.getName() + " interface.");
		}
	}

	public void setSavepointRestoreSettings(SavepointRestoreSettings savepointSettings) {
		this.savepointSettings = savepointSettings;
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

	public boolean isUsingInteractiveMode() {
		return this.program == null;
	}

	public boolean isUsingProgramEntryPoint() {
		return this.program != null;
	}

	/**
	 * Returns the plan without the required jars when the files are already provided by the cluster.
	 *
	 * @return The plan without attached jar files.
	 * @throws ProgramInvocationException
	 */
	public JobWithJars getPlanWithoutJars() throws ProgramInvocationException {
		if (isUsingProgramEntryPoint()) {
			return new JobWithJars(getPlan(), Collections.<URL>emptyList(), classpaths, userCodeClassLoader);
		} else {
			throw new ProgramInvocationException("Cannot create a " + JobWithJars.class.getSimpleName() +
				" for a program that is using the interactive mode.", getPlan().getJobId());
		}
	}

	/**
	 * Returns the plan with all required jars.
	 *
	 * @return The plan with attached jar files.
	 * @throws ProgramInvocationException
	 */
	public JobWithJars getPlanWithJars() throws ProgramInvocationException {
		if (isUsingProgramEntryPoint()) {
			return new JobWithJars(getPlan(), getAllLibraries(), classpaths, userCodeClassLoader);
		} else {
			throw new ProgramInvocationException("Cannot create a " + JobWithJars.class.getSimpleName() +
					" for a program that is using the interactive mode.", getPlan().getJobId());
		}
	}

	/**
	 * Returns the analyzed plan without any optimizations.
	 *
	 * @return
	 *         the analyzed plan without any optimizations.
	 * @throws ProgramInvocationException Thrown if an error occurred in the
	 *  user-provided pact assembler. This may indicate
	 *         missing parameters for generation.
	 */
	public String getPreviewPlan() throws ProgramInvocationException {
		Thread.currentThread().setContextClassLoader(this.getUserCodeClassLoader());
		List<DataSinkNode> previewPlan;

		if (isUsingProgramEntryPoint()) {
			previewPlan = Optimizer.createPreOptimizedPlan(getPlan());
		}
		else if (isUsingInteractiveMode()) {
			// temporary hack to support the web client
			PreviewPlanEnvironment env = new PreviewPlanEnvironment();
			env.setAsContext();
			try {
				invokeInteractiveModeForExecution();
			}
			catch (ProgramInvocationException e) {
				throw e;
			}
			catch (Throwable t) {
				// the invocation gets aborted with the preview plan
				if (env.previewPlan == null) {
					if (env.preview != null) {
						return env.preview;
					} else {
						throw new ProgramInvocationException("The program caused an error: ", getPlan().getJobId(), t);
					}
				}
			}
			finally {
				env.unsetAsContext();
			}

			if (env.previewPlan != null) {
				previewPlan =  env.previewPlan;
			} else {
				throw new ProgramInvocationException(
					"The program plan could not be fetched. The program silently swallowed the control flow exceptions.",
					getPlan().getJobId());
			}
		}
		else {
			throw new RuntimeException();
		}

		PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
		StringWriter string = new StringWriter(1024);
		try (PrintWriter pw = new PrintWriter(string)) {
			jsonGen.dumpPactPlanAsJSON(previewPlan, pw);
		}
		return string.toString();

	}

	/**
	 * Returns the description provided by the Program class. This
	 * may contain a description of the plan itself and its arguments.
	 *
	 * @return The description of the PactProgram's input parameters.
	 * @throws ProgramInvocationException
	 *         This invocation is thrown if the Program can't be properly loaded. Causes
	 *         may be a missing / wrong class or manifest files.
	 */
	@Nullable
	public String getDescription() throws ProgramInvocationException {
		if (ProgramDescription.class.isAssignableFrom(this.mainClass)) {

			ProgramDescription descr;
			if (this.program != null) {
				descr = (ProgramDescription) this.program;
			} else {
				try {
					descr =  InstantiationUtil.instantiate(
						this.mainClass.asSubclass(ProgramDescription.class), ProgramDescription.class);
				} catch (Throwable t) {
					return null;
				}
			}

			try {
				return descr.getDescription();
			}
			catch (Throwable t) {
				throw new ProgramInvocationException("Error while getting the program description" +
						(t.getMessage() == null ? "." : ": " + t.getMessage()), t);
			}

		} else {
			return null;
		}
	}

	/**
	 * This method assumes that the context environment is prepared, or the execution
	 * will be a local execution by default.
	 */
	public void invokeInteractiveModeForExecution() throws ProgramInvocationException{
		if (isUsingInteractiveMode()) {
			callMainMethod(mainClass, args);
		} else {
			throw new ProgramInvocationException("Cannot invoke a plan-based program directly.");
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

	/**
	 * Returns all provided libraries needed to run the program.
	 */
	public List<URL> getAllLibraries() {
		List<URL> libs = new ArrayList<URL>(this.extractedTempLibraries.size() + 1);

		if (jarFile != null) {
			libs.add(jarFile);
		}
		for (File tmpLib : this.extractedTempLibraries) {
			try {
				libs.add(tmpLib.getAbsoluteFile().toURI().toURL());
			}
			catch (MalformedURLException e) {
				throw new RuntimeException("URL is invalid. This should not happen.", e);
			}
		}

		if (isPython) {
			String flinkOptPath = System.getenv(ConfigConstants.ENV_FLINK_OPT_DIR);
			final List<Path> pythonJarPath = new ArrayList<>();
			try {
				Files.walkFileTree(FileSystems.getDefault().getPath(flinkOptPath), new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
						FileVisitResult result = super.visitFile(file, attrs);
						if (file.getFileName().toString().startsWith("flink-python")) {
							pythonJarPath.add(file);
						}
						return result;
					}
				});
			} catch (IOException e) {
				throw new RuntimeException(
					"Exception encountered during finding the flink-python jar. This should not happen.", e);
			}

			if (pythonJarPath.size() != 1) {
				throw new RuntimeException("Found " + pythonJarPath.size() + " flink-python jar.");
			}

			try {
				libs.add(pythonJarPath.get(0).toUri().toURL());
			} catch (MalformedURLException e) {
				throw new RuntimeException("URL is invalid. This should not happen.", e);
			}
		}

		return libs;
	}

	/**
	 * Deletes all temporary files created for contained packaged libraries.
	 */
	public void deleteExtractedLibraries() {
		deleteExtractedLibraries(this.extractedTempLibraries);
		this.extractedTempLibraries.clear();
	}

	/**
	 * Returns the plan as generated from the Pact Assembler.
	 *
	 * @return The program's plan.
	 * @throws ProgramInvocationException Thrown, if an error occurred in the program while
	 *         creating the program's {@link Plan}.
	 */
	private Plan getPlan() throws ProgramInvocationException {
		if (this.plan == null) {
			Thread.currentThread().setContextClassLoader(this.userCodeClassLoader);
			this.plan = createPlanFromProgram(this.program, this.args);
		}

		return this.plan;
	}

	private static boolean hasMainMethod(Class<?> entryClass) {
		Method mainMethod;
		try {
			mainMethod = entryClass.getMethod("main", String[].class);
		} catch (NoSuchMethodException e) {
			return false;
		}
		catch (Throwable t) {
			throw new RuntimeException("Could not look up the main(String[]) method from the class " +
					entryClass.getName() + ": " + t.getMessage(), t);
		}

		return Modifier.isStatic(mainMethod.getModifiers()) && Modifier.isPublic(mainMethod.getModifiers());
	}

	private static void callMainMethod(Class<?> entryClass, String[] args) throws ProgramInvocationException {
		Method mainMethod;
		if (!Modifier.isPublic(entryClass.getModifiers())) {
			throw new ProgramInvocationException("The class " + entryClass.getName() + " must be public.");
		}

		try {
			mainMethod = entryClass.getMethod("main", String[].class);
		} catch (NoSuchMethodException e) {
			throw new ProgramInvocationException("The class " + entryClass.getName() + " has no main(String[]) method.");
		}
		catch (Throwable t) {
			throw new ProgramInvocationException("Could not look up the main(String[]) method from the class " +
					entryClass.getName() + ": " + t.getMessage(), t);
		}

		if (!Modifier.isStatic(mainMethod.getModifiers())) {
			throw new ProgramInvocationException("The class " + entryClass.getName() + " declares a non-static main method.");
		}
		if (!Modifier.isPublic(mainMethod.getModifiers())) {
			throw new ProgramInvocationException("The class " + entryClass.getName() + " declares a non-public main method.");
		}

		try {
			mainMethod.invoke(null, (Object) args);
		}
		catch (IllegalArgumentException e) {
			throw new ProgramInvocationException("Could not invoke the main method, arguments are not matching.", e);
		}
		catch (IllegalAccessException e) {
			throw new ProgramInvocationException("Access to the main method was denied: " + e.getMessage(), e);
		}
		catch (InvocationTargetException e) {
			Throwable exceptionInMethod = e.getTargetException();
			if (exceptionInMethod instanceof Error) {
				throw (Error) exceptionInMethod;
			} else if (exceptionInMethod instanceof ProgramParametrizationException) {
				throw (ProgramParametrizationException) exceptionInMethod;
			} else if (exceptionInMethod instanceof ProgramInvocationException) {
				throw (ProgramInvocationException) exceptionInMethod;
			} else {
				throw new ProgramInvocationException("The main method caused an error: " + exceptionInMethod.getMessage(), exceptionInMethod);
			}
		}
		catch (Throwable t) {
			throw new ProgramInvocationException("An error occurred while invoking the program's main method: " + t.getMessage(), t);
		}
	}

	private static String getEntryPointClassNameFromJar(URL jarFile) throws ProgramInvocationException {
		JarFile jar;
		Manifest manifest;
		String className;

		// Open jar file
		try {
			jar = new JarFile(new File(jarFile.toURI()));
		} catch (URISyntaxException use) {
			throw new ProgramInvocationException("Invalid file path '" + jarFile.getPath() + "'", use);
		} catch (IOException ioex) {
			throw new ProgramInvocationException("Error while opening jar file '" + jarFile.getPath() + "'. "
				+ ioex.getMessage(), ioex);
		}

		// jar file must be closed at the end
		try {
			// Read from jar manifest
			try {
				manifest = jar.getManifest();
			} catch (IOException ioex) {
				throw new ProgramInvocationException("The Manifest in the jar file could not be accessed '"
					+ jarFile.getPath() + "'. " + ioex.getMessage(), ioex);
			}

			if (manifest == null) {
				throw new ProgramInvocationException("No manifest found in jar file '" + jarFile.getPath() + "'. The manifest is need to point to the program's main class.");
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
				throw new ProgramInvocationException("Neither a '" + MANIFEST_ATTRIBUTE_MAIN_CLASS + "', nor a '" +
						MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS + "' entry was found in the jar file.");
			}
		}
		finally {
			try {
				jar.close();
			} catch (Throwable t) {
				throw new ProgramInvocationException("Could not close the JAR file: " + t.getMessage(), t);
			}
		}
	}

	private static Class<?> loadMainClass(String className, ClassLoader cl) throws ProgramInvocationException {
		ClassLoader contextCl = null;
		try {
			contextCl = Thread.currentThread().getContextClassLoader();
			Thread.currentThread().setContextClassLoader(cl);
			return Class.forName(className, false, cl);
		}
		catch (ClassNotFoundException e) {
			throw new ProgramInvocationException("The program's entry point class '" + className
				+ "' was not found in the jar file.", e);
		}
		catch (ExceptionInInitializerError e) {
			throw new ProgramInvocationException("The program's entry point class '" + className
				+ "' threw an error during initialization.", e);
		}
		catch (LinkageError e) {
			throw new ProgramInvocationException("The program's entry point class '" + className
				+ "' could not be loaded due to a linkage failure.", e);
		}
		catch (Throwable t) {
			throw new ProgramInvocationException("The program's entry point class '" + className
				+ "' caused an exception during initialization: " + t.getMessage(), t);
		} finally {
			if (contextCl != null) {
				Thread.currentThread().setContextClassLoader(contextCl);
			}
		}
	}

	/**
	 * Takes the jar described by the given file and invokes its pact assembler class to
	 * assemble a plan. The assembler class name is either passed through a parameter,
	 * or it is read from the manifest of the jar. The assembler is handed the given options
	 * for its assembly.
	 *
	 * @param program The program to create the plan for.
	 * @param options
	 *        The options for the assembler.
	 * @return The plan created by the program.
	 * @throws ProgramInvocationException
	 *         Thrown, if an error occurred in the user-provided pact assembler.
	 */
	private static Plan createPlanFromProgram(Program program, String[] options) throws ProgramInvocationException {
		try {
			return program.getPlan(options);
		} catch (Throwable t) {
			throw new ProgramInvocationException("Error while calling the program: " + t.getMessage(), t);
		}
	}

	/**
	 * Takes all JAR files that are contained in this program's JAR file and extracts them
	 * to the system's temp directory.
	 *
	 * @return The file names of the extracted temporary files.
	 * @throws ProgramInvocationException Thrown, if the extraction process failed.
	 */
	public static List<File> extractContainedLibraries(URL jarFile) throws ProgramInvocationException {

		Random rnd = new Random();

		JarFile jar = null;
		try {
			jar = new JarFile(new File(jarFile.toURI()));
			final List<JarEntry> containedJarFileEntries = new ArrayList<JarEntry>();

			Enumeration<JarEntry> entries = jar.entries();
			while (entries.hasMoreElements()) {
				JarEntry entry = entries.nextElement();
				String name = entry.getName();

				if (name.length() > 8 && name.startsWith("lib/") && name.endsWith(".jar")) {
					containedJarFileEntries.add(entry);
				}
			}

			if (containedJarFileEntries.isEmpty()) {
				return Collections.emptyList();
			}
			else {
				// go over all contained jar files
				final List<File> extractedTempLibraries = new ArrayList<File>(containedJarFileEntries.size());
				final byte[] buffer = new byte[4096];

				boolean incomplete = true;

				try {
					for (int i = 0; i < containedJarFileEntries.size(); i++) {
						final JarEntry entry = containedJarFileEntries.get(i);
						String name = entry.getName();
						// '/' as in case of zip, jar
						// java.util.zip.ZipEntry#isDirectory always looks only for '/' not for File.separator
						name = name.replace('/', '_');

						File tempFile;
						try {
							tempFile = File.createTempFile(rnd.nextInt(Integer.MAX_VALUE) + "_", name);
							tempFile.deleteOnExit();
						}
						catch (IOException e) {
							throw new ProgramInvocationException(
								"An I/O error occurred while creating temporary file to extract nested library '" +
										entry.getName() + "'.", e);
						}

						extractedTempLibraries.add(tempFile);

						// copy the temp file contents to a temporary File
						OutputStream out = null;
						InputStream in = null;
						try {

							out = new FileOutputStream(tempFile);
							in = new BufferedInputStream(jar.getInputStream(entry));

							int numRead = 0;
							while ((numRead = in.read(buffer)) != -1) {
								out.write(buffer, 0, numRead);
							}
						}
						catch (IOException e) {
							throw new ProgramInvocationException("An I/O error occurred while extracting nested library '"
									+ entry.getName() + "' to temporary file '" + tempFile.getAbsolutePath() + "'.");
						}
						finally {
							if (out != null) {
								out.close();
							}
							if (in != null) {
								in.close();
							}
						}
					}

					incomplete = false;
				}
				finally {
					if (incomplete) {
						deleteExtractedLibraries(extractedTempLibraries);
					}
				}

				return extractedTempLibraries;
			}
		}
		catch (Throwable t) {
			throw new ProgramInvocationException("Unknown I/O error while extracting contained jar files.", t);
		}
		finally {
			if (jar != null) {
				try {
					jar.close();
				} catch (Throwable t) {}
			}
		}
	}

	public static void deleteExtractedLibraries(List<File> tempLibraries) {
		for (File f : tempLibraries) {
			f.delete();
		}
	}

	private static void checkJarFile(URL jarfile) throws ProgramInvocationException {
		try {
			JobWithJars.checkJarFile(jarfile);
		}
		catch (IOException e) {
			throw new ProgramInvocationException(e.getMessage(), e);
		}
		catch (Throwable t) {
			throw new ProgramInvocationException("Cannot access jar file" + (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
		}
	}

}
