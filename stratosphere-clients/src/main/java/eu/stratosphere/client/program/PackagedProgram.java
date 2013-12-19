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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.dag.DataSinkNode;

/**
 * This class encapsulates most of the plan related functions. Based on the given jar file,
 * pact assembler class and arguments it can create the plans necessary for execution, or
 * also provide a description of the plan if available.
 */
public class PackagedProgram {

	/**
	 * Property name of the pact assembler definition in the JAR manifest file.
	 */
	public static final String MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS = "program-class";

	// --------------------------------------------------------------------------------------------

	private final File jarFile;

	private final String[] args;
	
	private final Program planAssembler;
	
	private List<File> extractedTempLibraries;
	
	private ClassLoader userCodeClassLoader;
	
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
		checkJarFile(jarFile);
		
		this.jarFile = jarFile;
		this.args = args == null ? new String[0] : args;
		
		this.extractedTempLibraries = extractContainedLibaries(jarFile);
		this.userCodeClassLoader = buildUserCodeClassLoader(jarFile, extractedTempLibraries, getClass().getClassLoader());
		
		Class<? extends Program> assemblerClass = getPactAssemblerFromJar(jarFile, userCodeClassLoader);
		this.planAssembler = instantiateAssemblerFromClass(assemblerClass);
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
	public PackagedProgram(File jarFile, String className, String... args) throws ProgramInvocationException {
		checkJarFile(jarFile);
		
		this.jarFile = jarFile;
		this.args = args == null ? new String[0] : args;
		
		this.extractedTempLibraries = extractContainedLibaries(jarFile);
		this.userCodeClassLoader = buildUserCodeClassLoader(jarFile, extractedTempLibraries, getClass().getClassLoader());
		
		Class<? extends Program> assemblerClass = getPactAssemblerFromJar(jarFile, className, userCodeClassLoader);
		this.planAssembler = instantiateAssemblerFromClass(assemblerClass);
	}
	
	

	/**
	 * Returns the plan with all required jars.
	 * @throws IOException 
	 * @throws JobInstantiationException 
	 * @throws ProgramInvocationException 
	 */
	public JobWithJars getPlanWithJars() throws ProgramInvocationException, JobInstantiationException, IOException {
		List<File> allJars = new ArrayList<File>();
		
		allJars.add(jarFile);
		allJars.addAll(extractedTempLibraries);
		
		return new JobWithJars(getPlan(), allJars, userCodeClassLoader);
	}

	/**
	 * Returns the analyzed plan without any optimizations.
	 * 
	 * @return
	 *         the analyzed plan without any optimizations.
	 * @throws ProgramInvocationException
	 *         This invocation is thrown if the Program can't be properly loaded. Causes
	 *         may be a missing / wrong class or manifest files.
	 * @throws JobInstantiationException
	 *         Thrown if an error occurred in the user-provided pact assembler. This may indicate
	 *         missing parameters for generation.
	 */
	public List<DataSinkNode> getPreviewPlan() throws ProgramInvocationException, JobInstantiationException {
		Plan plan = getPlan();
		if (plan != null) {
			return PactCompiler.createPreOptimizedPlan(plan);
		} else {
			return null;
		}
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
		if (this.planAssembler instanceof ProgramDescription) {
			try {
				return ((ProgramDescription) this.planAssembler).getDescription();
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
	 * Gets the {@link java.lang.ClassLoader} that must be used to load user code classes.
	 * 
	 * @return The user code ClassLoader.
	 */
	public ClassLoader getUserCodeClassLoader() {
		return this.userCodeClassLoader;
	}
	
	/**
	 * Deletes all temporary files created for contained packaged libraries.
	 */
	public void deleteExtractedLibraries() {
		List<File> files = this.extractedTempLibraries;
		
		this.userCodeClassLoader = null;
		this.extractedTempLibraries = null;
		
		if (files != null) {
			deleteExtractedLibraries(files);
		}
	}
	
	
	/**
	 * Returns the plan as generated from the Pact Assembler.
	 * 
	 * @return
	 *         the generated plan
	 * @throws ProgramInvocationException
	 *         This invocation is thrown if the Program can't be properly loaded. Causes
	 *         may be a missing / wrong class or manifest files.
	 * @throws JobInstantiationException
	 *         Thrown if an error occurred in the user-provided pact assembler. This may indicate
	 *         missing parameters for generation.
	 */
	private Plan getPlan() throws ProgramInvocationException, JobInstantiationException {
		if (this.plan == null) {
			this.plan = createPlanFromProgram(this.planAssembler, this.args);
		}
		
		return this.plan;
	}

	private static Class<? extends Program> getPactAssemblerFromJar(File jarFile, ClassLoader cl)
			throws ProgramInvocationException
	{
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

		// Read from jar manifest
		try {
			manifest = jar.getManifest();
		} catch (IOException ioex) {
			throw new ProgramInvocationException("The Manifest in the JAR file could not be accessed '"
				+ jarFile.getPath() + "'. " + ioex.getMessage(), ioex);
		}

		if (manifest == null) {
			throw new ProgramInvocationException("No manifest found in jar '" + jarFile.getPath() + "'");
		}

		Attributes attributes = manifest.getMainAttributes();
		className = attributes.getValue(PackagedProgram.MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS);

		if (className == null) {
			throw new ProgramInvocationException("No plan assembler class defined in manifest");
		}

		try {
			jar.close();
		} catch (IOException ex) {
			throw new ProgramInvocationException("Could not close JAR. " + ex.getMessage(), ex);
		}

		return getPactAssemblerFromJar(jarFile, className, cl);
	}

	private static Class<? extends Program> getPactAssemblerFromJar(File jarFile, String className, ClassLoader cl)
			throws ProgramInvocationException
	{
		try {
			return Class.forName(className, true, cl).asSubclass(Program.class);
		}
		catch (ClassNotFoundException e) {
			throw new ProgramInvocationException("The pact plan assembler class '" + className
				+ "' was not found in the jar file '" + jarFile.getPath() + "'.", e);
		}
		catch (ClassCastException e) {
			throw new ProgramInvocationException("The pact plan assembler class '" + className
				+ "' cannot be cast to Program.", e);
		}
		catch (Throwable t) {
			throw new ProgramInvocationException("An unknown problem ocurred during the instantiation of the "
				+ "program assembler: " + t, t);
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
	 * @return The plan created by the assembler.
	 * @throws ProgramInvocationException
	 *         Thrown, if the jar file or its manifest could not be accessed, or if the assembler
	 *         class was not found or could not be instantiated.
	 * @throws JobInstantiationException
	 *         Thrown, if an error occurred in the user-provided pact assembler.
	 */
	private static Plan createPlanFromProgram(Program assembler, String[] options)
			throws ProgramInvocationException, JobInstantiationException
	{
		try {
			return assembler.getPlan(options);
		} catch (Throwable t) {
			throw new JobInstantiationException("Error while creating plan: " + t, t);
		}
	}
	
	/**
	 * Instantiates the given plan assembler class
	 * 
	 * @param clazz
	 *        class that should be instantiated.
	 * @return
	 *         instance of the class
	 * @throws ProgramInvocationException
	 *         is thrown if class can't be found or instantiated
	 */
	private static Program instantiateAssemblerFromClass(Class<? extends Program> clazz)
			throws ProgramInvocationException
	{
		try {
			return clazz.newInstance();
		} catch (InstantiationException e) {
			throw new ProgramInvocationException("ERROR: The pact plan assembler class could not be instantiated. "
				+ "Make sure that the class is a proper class (not abstract/interface) and has a "
				+ "public constructor with no arguments.", e);
		} catch (IllegalAccessException e) {
			throw new ProgramInvocationException("ERROR: The pact plan assembler class could not be instantiated. "
				+ "Make sure that the class has a public constructor with no arguments.", e);
		} catch (Throwable t) {
			throw new ProgramInvocationException("An error ocurred during the instantiation of the "
				+ "program assembler: " + t.getMessage(), t);
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
		
		try {
			final JarFile jar = new JarFile(jarFile);
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
}
