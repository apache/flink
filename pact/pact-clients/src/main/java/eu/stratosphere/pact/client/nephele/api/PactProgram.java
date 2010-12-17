/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.client.nephele.api;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.FixedSizeClusterCostEstimator;
import eu.stratosphere.pact.compiler.jobgen.JobGraphGenerator;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.contextcheck.ContextChecker;

/**
 * This class encapsulates most of the plan related functions. Based on the given jar file,
 * pact assembler class and arguments it can create the plans necessary for execution, or
 * also provide a description of the plan if available.
 * 
 * @author Moritz Kaufmann
 */
public class PactProgram {
	public static final String MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS = "Pact-Assembler-Class";

	private final Class<? extends PlanAssembler> assemblerClass;

	private final File jarFile;

	private final String[] args;

	/**
	 * Creates an instance that wraps the plan defined in the jar file using the given
	 * argument.
	 * 
	 * @param jarFile
	 *        The jar file which contains the plan and a Manifest which defines
	 *        the Pact-Assembler-Class
	 * @param args
	 *        Optional. The arguments used to create the pact plan, depend on
	 *        implementation of the pact plan. See getDescription().
	 * @throws ProgramInvocationException
	 */
	public PactProgram(File jarFile, String... args)
													throws ProgramInvocationException {
		this.assemblerClass = getPactAssemblerFromJar(jarFile);
		this.jarFile = jarFile;
		this.args = args;
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
	 */
	public PactProgram(File jarFile, String className, String... args)
																		throws ProgramInvocationException {
		this.assemblerClass = getPactAssemblerFromJar(jarFile, className);
		this.jarFile = jarFile;
		this.args = args;
	}

	/**
	 * Returns the plan as generated from the Pact Assembler.
	 */
	public Plan getPlan() throws ProgramInvocationException, ErrorInPlanAssemblerException {
		return createPlanFromJar(assemblerClass, args);
	}

	/**
	 * Semantic check of generated plan
	 * 
	 * @throws ProgramInvocationException
	 * @throws ErrorInPlanAssemblerException
	 */
	public void checkPlan() throws ProgramInvocationException, ErrorInPlanAssemblerException {
		// semantic context check of the generated plan
		ContextChecker checker = new ContextChecker();
		checker.check(getPlan());
	}

	/**
	 * Returns the analyzed plan without any optimizations.
	 * 
	 * @return the analyzed plan without any optimizations.
	 * @throws ProgramInvocationException
	 * @throws ErrorInPlanAssemblerException
	 */
	public OptimizedPlan getPreOptimizedPlan() throws ProgramInvocationException, ErrorInPlanAssemblerException {
		Plan plan = getPlan();
		if(plan != null) {
			return getPreOptimizedPlan(plan);
		} else {
			return null;
		}
	}

	/**
	 * Returns the optimized plan, based on input file sizes and cluster
	 * configuration.
	 */
	public OptimizedPlan getOptimizedPlan() throws ProgramInvocationException, ErrorInPlanAssemblerException {
		return getOptimizedPlan(getPlan());
	}

	/**
	 * Returns the JobGraph corresponding to the generated optimized plan.
	 * The JobGraph can be send to the nephele cluster for execution.
	 * 
	 * @return
	 * @throws ProgramInvocationException
	 * @throws ErrorInPlanAssemblerException
	 */
	public JobGraph getCompiledPlan() throws ProgramInvocationException, ErrorInPlanAssemblerException {
		return getCompiledPlan(getOptimizedPlan());
	}

	/**
	 * Returns the File object of the jar file that is used as base for the
	 * pact program.
	 * 
	 * @return
	 */
	public File getJarFile() {
		return jarFile;
	}

	/**
	 * Returns the description provided by the PlanAssembler class. This
	 * may contain a description of the plan itself and its arguments.
	 * 
	 * @return
	 * @throws ProgramInvocationException
	 * @throws ErrorInPlanAssemblerException
	 */
	public String getDescription() throws ProgramInvocationException {
		PlanAssembler assembler = createAssemblerFromJar(assemblerClass);
		if (assembler instanceof PlanAssemblerDescription) {
			return ((PlanAssemblerDescription) assembler).getDescription();
		} else {
			return null;
		}
	}

	/**
	 * Returns the description provided by the PlanAssembler class without
	 * any HTML tags. This may contain a description of the plan itself
	 * and its arguments.
	 * 
	 * @return
	 * @throws ProgramInvocationException
	 * @throws ErrorInPlanAssemblerException
	 */
	public String getTextDescription() throws ProgramInvocationException {
		String descr = getDescription();
		if (descr == null || descr.length() == 0) {
			return null;
		} else {
			final Pattern BREAK_TAGS = Pattern.compile("<(b|B)(r|R) */?>");
			final Pattern REMOVE_TAGS = Pattern.compile("<.+?>");
			Matcher m = BREAK_TAGS.matcher(descr);
			descr = m.replaceAll("\n");
			m = REMOVE_TAGS.matcher(descr);
			// TODO: Properly convert &amp; etc
			return m.replaceAll("");
		}
	}

	protected OptimizedPlan getPreOptimizedPlan(Plan plan) {
		// TODO: Can this be instantiated statically?
		PactCompiler compiler = new PactCompiler();

		// perform the actual compilation
		OptimizedPlan optPlan = compiler.createPreOptimizedPlan(plan);
		return optPlan;
	}

	protected OptimizedPlan getOptimizedPlan(Plan plan) {
		// TODO: Can this be instantiated statically?
		PactCompiler compiler = new PactCompiler(new DataStatistics(), new FixedSizeClusterCostEstimator());

		// perform the actual compilation
		OptimizedPlan optPlan = compiler.compile(plan);
		return optPlan;
	}

	protected JobGraph getCompiledPlan(OptimizedPlan optPlan) {
		JobGraph jobGraph = null;

		// now run the code generator that creates the nephele schedule
		JobGraphGenerator codeGen = new JobGraphGenerator();
		jobGraph = codeGen.compileJobGraph(optPlan);

		return jobGraph;
	}

	/**
	 * Takes the jar described by the given file and invokes its pact assembler class to
	 * assemble a plan. The assembler class name is either passed through a parameter,
	 * or it is read from the manifest of the jar. The assembler is handed the given options
	 * for its assembly.
	 * 
	 * @param jarFile
	 *        The path to the jar file.
	 * @param mainClassName
	 *        The name of the assembler class, or null, if the class should be read from
	 *        the manifest.
	 * @param options
	 *        The options for the assembler.
	 * @return The plan created by the assembler.
	 * @throws ProgramInvocationException
	 *         Thrown, if the jar file or its manifest could not be accessed, or if the assembler
	 *         class was not found or could not be instantiated.
	 * @throws ErrorInPlanAssemblerException
	 *         Thrown, if an error occurred in the user-provided pact assembler.
	 */
	protected Plan createPlanFromJar(Class<? extends PlanAssembler> clazz, String[] options)
			throws ProgramInvocationException, ErrorInPlanAssemblerException {
		PlanAssembler assembler = createAssemblerFromJar(clazz);

		// run the user-provided assembler class
		try {
			return assembler.getPlan(options);
		} catch (Throwable t) {
			throw new ErrorInPlanAssemblerException("Error while creating plan: " + t, t);
		}
	}

	protected PlanAssembler createAssemblerFromJar(Class<? extends PlanAssembler> clazz)
			throws ProgramInvocationException {
		// we have the class. now create a classloader that can load the
		// contents of the jar
		PlanAssembler assembler = null;
		try {
			assembler = clazz.newInstance();
		} catch (InstantiationException e) {
			throw new ProgramInvocationException("ERROR: The pact plan assembler class could not be instantiated. "
				+ "Make sure that the class is a proper class (not abstract) and has a "
				+ "public constructor with no arguments.", e);
		} catch (IllegalAccessException e) {
			throw new ProgramInvocationException("ERROR: The pact plan assembler class could not be instantiated. "
				+ "Make sure that the class has a public constructor with no arguments.", e);
		} catch (Throwable t) {
			throw new ProgramInvocationException("An unknown problem ocurred during the instantiation of the "
				+ "program assembler: " + t.getMessage(), t);
		}

		return assembler;
	}

	protected JobGraph compilePlan(Plan pactPlan) {
		// TODO: Can this be instantiated statically?
		PactCompiler compiler = new PactCompiler(new DataStatistics(), new FixedSizeClusterCostEstimator());

		// perform the actual compilation
		OptimizedPlan optPlan = null;
		JobGraph jobGraph = null;

		// first run the pact compiler and optimizer
		optPlan = compiler.compile(pactPlan);

		// now run the code generator that creates the nephele schedule
		JobGraphGenerator codeGen = new JobGraphGenerator();
		jobGraph = codeGen.compileJobGraph(optPlan);

		return jobGraph;
	}

	private Class<? extends PlanAssembler> getPactAssemblerFromJar(File jarFile) throws ProgramInvocationException {
		JarFile jar = null;
		Manifest manifest = null;
		String className = null;

		checkJarFile(jarFile);

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
		className = attributes.getValue(PactProgram.MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS);

		if (className == null) {
			throw new ProgramInvocationException("No plan assembler class defined in manifest");
		}

		try {
			jar.close();
		} catch (IOException ex) {
			throw new ProgramInvocationException("Could not close JAR. " + ex.getMessage(), ex);
		}

		return getPactAssemblerFromJar(jarFile, className);
	}

	private Class<? extends PlanAssembler> getPactAssemblerFromJar(File jarFile, String className)
			throws ProgramInvocationException {
		Class<? extends PlanAssembler> clazz = null;

		checkJarFile(jarFile);

		try {
			URL url = jarFile.getAbsoluteFile().toURI().toURL();
			ClassLoader loader = new URLClassLoader(new URL[] { url }, this.getClass().getClassLoader());
			clazz = Class.forName(className, true, loader).asSubclass(PlanAssembler.class);
		} catch (MalformedURLException e) {
			throw new ProgramInvocationException(
				"The given JAR file could not be translated to a valid URL for class access.", e);
		} catch (ClassNotFoundException e) {
			throw new ProgramInvocationException("The pact plan assembler class '" + className
				+ "' was not found in the jar file '" + jarFile.getPath() + "'.", e);
		} catch (ClassCastException e) {
			throw new ProgramInvocationException("The pact plan assembler class '" + className
				+ "' cannot be cast to PlanAssembler.", e);
		} catch (Throwable t) {
			throw new ProgramInvocationException("An unknown problem ocurred during the instantiation of the "
				+ "program assembler: " + t, t);
		}

		return clazz;
	}

	private void checkJarFile(File jar) throws ProgramInvocationException {
		if (!jar.exists()) {
			throw new ProgramInvocationException("JAR file does not exist '" + jarFile.getPath() + "'");

		}
		if (!jar.canRead()) {
			throw new ProgramInvocationException("JAR file can't be read '" + jarFile.getPath() + "'");
		}

		// TODO: Check if proper JAR file
	}
}
