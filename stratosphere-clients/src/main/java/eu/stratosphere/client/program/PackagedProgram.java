/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.client.program;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.dag.DataSinkNode;
import eu.stratosphere.compiler.plandump.PlanJSONDumpGenerator;
import eu.stratosphere.util.InstantiationUtil;

/**
 * This class encapsulates represents a program, packaged in a jar file. It supplies
 * functionality to extract nested libraries, search for the program entry point, and extract
 * a program plan.
 */
public class PackagedProgram {

	/**
	 * Property name of the entry in JAR manifest file that describes the stratosphere specific entry point.
	 */
	public static final String MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS = "program-class";
	
	/**
	 * Property name of the entry in JAR manifest file that describes the class with the main method.
	 */
	public static final String MANIFEST_ATTRIBUTE_MAIN_CLASS = "Main-Class";

	// --------------------------------------------------------------------------------------------

	private final File jarFile;

	private final String[] args;
	
	private final Program program;
	
	private final Class<?> mainClass;
	
	private final List<File> extractedTempLibraries;
	
	private final ClassLoader userCodeClassLoader;
	
	private Plan plan;

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
		this(jarFile, null, args);
	}

	/**
	 * Creates an instance that wraps the plan defined in the jar file using the given
	 * arguments. For generating the plan the class defined in the className parameter
	 * is used.
	 * 
	 * @param jarFile
	 *        The jar file which contains the plan.
	 * @param className
	 *        Name of the class which generates the plan. Overrides the class defined
	 *        in the jar file manifest
	 * @param args
	 *        Optional. The arguments used to create the pact plan, depend on
	 *        implementation of the pact plan. See getDescription().
	 * @throws ProgramInvocationException
	 *         This invocation is thrown if the Program can't be properly loaded. Causes
	 *         may be a missing / wrong class or manifest files.
	 */
	public PackagedProgram(File jarFile, String entryPointClassName, String... args) throws ProgramInvocationException {
		if (jarFile == null) {
			throw new IllegalArgumentException("The jar file must not be null.");
		}
		
		checkJarFile(jarFile);
		
		this.jarFile = jarFile;
		this.args = args == null ? new String[0] : args;
		
		// if no entryPointClassName name was given, we try and look one up through the manifest
		if (entryPointClassName == null) {
			entryPointClassName = getEntryPointClassNameFromJar(jarFile);
		}
		
		// now that we have an entry point, we can extract the nested jar files (if any)
		this.extractedTempLibraries = extractContainedLibaries(jarFile);
		this.userCodeClassLoader = buildUserCodeClassLoader(jarFile, extractedTempLibraries, getClass().getClassLoader());
		
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
	 * Returns the plan with all required jars.
	 * @throws JobInstantiationException 
	 * @throws ProgramInvocationException 
	 */
	public JobWithJars getPlanWithJars() throws ProgramInvocationException {
		if (isUsingProgramEntryPoint()) {
			List<File> allJars = new ArrayList<File>();
			
			allJars.add(jarFile);
			allJars.addAll(extractedTempLibraries);
			
			return new JobWithJars(getPlan(), allJars, userCodeClassLoader);
		} else {
			throw new ProgramInvocationException("Cannot create a " + JobWithJars.class.getSimpleName() + 
					" for a program that is using the interactive mode.");
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
		List<DataSinkNode> previewPlan;
		
		if (isUsingProgramEntryPoint()) {
			previewPlan = PactCompiler.createPreOptimizedPlan(getPlan());
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
				if (env.previewPlan != null) {
					previewPlan =  env.previewPlan;
				} else {
					throw new ProgramInvocationException("The program caused an error: ", t);
				}
			}
			
			if (env.previewPlan != null) {
				previewPlan =  env.previewPlan;
			} else {
				throw new ProgramInvocationException(
						"The program plan could not be fetched. The program silently swallowed the control flow exceptions.");
			}
		}
		else {
			throw new RuntimeException();
		}
		
		PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
		StringWriter string = new StringWriter(1024);
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(string);
			jsonGen.dumpPactPlanAsJSON(previewPlan, pw);
		} finally {
			pw.close();
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
	 * @throws JobInstantiationException
	 *         Thrown if an error occurred in the user-provided pact assembler. This may indicate
	 *         missing parameters for generation.
	 */
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
	 * 
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
	 * Gets the {@link java.lang.ClassLoader} that must be used to load user code classes.
	 * 
	 * @return The user code ClassLoader.
	 */
	public ClassLoader getUserCodeClassLoader() {
		return this.userCodeClassLoader;
	}
	
	public List<File> getAllLibraries() {
		List<File> libs = new ArrayList<File>(this.extractedTempLibraries.size() + 1);
		libs.add(jarFile);
		libs.addAll(this.extractedTempLibraries);
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
			} else if (exceptionInMethod instanceof ProgramInvocationException) {
				throw (ProgramInvocationException) exceptionInMethod;
			} else {
				throw new ProgramInvocationException("The main method caused an error.", exceptionInMethod);
			}
		}
		catch (Throwable t) {
			throw new ProgramInvocationException("An error occurred while invoking the program's main method: " + t.getMessage(), t);
		}
	}

	private static String getEntryPointClassNameFromJar(File jarFile) throws ProgramInvocationException {
		JarFile jar = null;
		Manifest manifest = null;
		String className = null;

		// Open jar file
		try {
			jar = new JarFile(jarFile);
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
		try {
			return Class.forName(className, true, cl);
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
				+ "' caused an exception during initialization: "+ t.getMessage(), t);
		}
	}
	
	/**
	 * Takes the jar described by the given file and invokes its pact assembler class to
	 * assemble a plan. The assembler class name is either passed through a parameter,
	 * or it is read from the manifest of the jar. The assembler is handed the given options
	 * for its assembly.
	 * 
	 * @param clazz
	 *        The name of the assembler class, or null, if the class should be read from
	 *        the manifest.
	 * @param options
	 *        The options for the assembler.
	 * @return The plan created by the program.
	 * @throws JobInstantiationException
	 *         Thrown, if an error occurred in the user-provided pact assembler.
	 */
	private static Plan createPlanFromProgram(Program assembler, String[] options) throws ProgramInvocationException {
		try {
			return assembler.getPlan(options);
		} catch (Throwable t) {
			throw new ProgramInvocationException("Error while calling the program: " + t.getMessage(), t);
		}
	}
	
	/**
	 * Takes all JAR files that are contained in this program's JAR file and extracts them
	 * to the system's temp directory.
	 * 
	 * @return The file names of the extracted temporary files.
	 * @throws IOException Thrown, if the extraction process failed.
	 */
	private static List<File> extractContainedLibaries(File jarFile) throws ProgramInvocationException {
		
		Random rnd = new Random();
		
		JarFile jar = null;
		try {
			jar = new JarFile(jarFile);
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
						name = name.replace(File.separatorChar, '_');
					
						File tempFile;
						try {
							tempFile = File.createTempFile(String.valueOf(Math.abs(rnd.nextInt()) + "_"), name);
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
	
	private static void deleteExtractedLibraries(List<File> tempLibraries) {
		for (File f : tempLibraries) {
			f.delete();
		}
	}
	
	private static ClassLoader buildUserCodeClassLoader(File mainJar, List<File> nestedJars, ClassLoader parent) throws ProgramInvocationException {
		ArrayList<File> allJars = new ArrayList<File>(nestedJars.size() + 1);
		allJars.add(mainJar);
		allJars.addAll(nestedJars);
		
		return JobWithJars.buildUserCodeClassLoader(allJars, parent);
	}
	
	private static void checkJarFile(File jarfile) throws ProgramInvocationException {
		try {
			JobWithJars.checkJarFile(jarfile);
		}
		catch (IOException e) {
			throw new ProgramInvocationException(e.getMessage());
		}
		catch (Throwable t) {
			throw new ProgramInvocationException("Cannot access jar file" + (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class PreviewPlanEnvironment extends ExecutionEnvironment {

		private List<DataSinkNode> previewPlan;
		
		@Override
		public JobExecutionResult execute(String jobName) throws Exception {
			Plan plan = createProgramPlan(jobName);
			this.previewPlan = PactCompiler.createPreOptimizedPlan(plan);
			
			// do not go on with anything now!
			throw new Client.ProgramAbortException();
		}

		@Override
		public String getExecutionPlan() throws Exception {
			Plan plan = createProgramPlan("unused");
			this.previewPlan = PactCompiler.createPreOptimizedPlan(plan);
			
			// do not go on with anything now!
			throw new Client.ProgramAbortException();
		}
		
		private void setAsContext() {
			initializeContextEnvironment(this);
		}
	}
}
