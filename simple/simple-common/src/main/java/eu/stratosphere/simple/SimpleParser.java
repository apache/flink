package eu.stratosphere.simple;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.antlr.runtime.BitSet;
import org.antlr.runtime.IntStream;
import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.MissingTokenException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.UnwantedTokenException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.sopremo.ExpressionTagFactory;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.OperatorFactory;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public abstract class SimpleParser extends Parser {
	protected OperatorFactory operatorFactory = new OperatorFactory();
	
	protected ExpressionTagFactory expressionTagFactory = new ExpressionTagFactory();

	protected List<Sink> sinks = new ArrayList<Sink>();

	public SimpleParser(TokenStream input, RecognizerSharedState state) {
		super(input, state);
		importPackage("base");
	}

	public SimpleParser(TokenStream input) {
		super(input);
		importPackage("base");
	}

	/**
	 * Imports sopremo-&lt;packageName&gt;.jar
	 * 
	 * @param packageName
	 */
	public void importPackage(String packageName) {
		String packagePath = getPackagePath(packageName);

		SimpleUtil.LOG.debug("adding package " + packagePath);
		try {
			if (packagePath.endsWith(".jar"))
				importFromJar(new File(packagePath).getAbsolutePath(), new File(packagePath));
			else
				// should only happen while debugging
				importFromProject(new File(packagePath).getAbsolutePath(), new File(packagePath));
		} catch (IOException e) {
			throw new IllegalArgumentException(String.format("could not load package %s", packagePath));
		}

	}

	protected String getPackagePath(String packageName) {
		String classpath = System.getProperty("java.class.path");
		String sopremoPackage = "sopremo-" + packageName;
		for (String path : classpath.split(File.pathSeparator))
			if (path.contains(sopremoPackage)) { // found entry
				return path;
			}
		throw new IllegalArgumentException(String.format("no package %s found", sopremoPackage));
	}

	private void importFromProject(String classPath, File dir) {
		for (File file : dir.listFiles())
			if (file.isDirectory())
				importFromProject(classPath, file);
			else if (file.getName().endsWith(".class") && !file.getName().contains("$"))
				importFromFile(classPath, file);
	}

	private void importFromFile(String classPath, File file) {
		String classFileName = file.getAbsolutePath().substring(classPath.length());
		String className = classFileName.replaceAll(".class$", "").replaceAll("/|\\\\", ".").replaceAll("^\\.", "");
		Class<?> clazz;
		try {
			clazz = Class.forName(className);
			if (Operator.class.isAssignableFrom(clazz) && (clazz.getModifiers() & Modifier.ABSTRACT) == 0) {
				SimpleUtil.LOG.trace("adding operator " + clazz);
				operatorFactory.addOperator((Class<? extends Operator>) clazz);
			}
		} catch (ClassNotFoundException e) {
			SimpleUtil.LOG.warn("could not load operator " + className);
		}
	}

	private void importFromJar(String classPath, File file) throws IOException {
		Enumeration<JarEntry> entries = new JarFile(file).entries();
		while (entries.hasMoreElements()) {
			JarEntry jarEntry = entries.nextElement();
			if (jarEntry.getName().endsWith(".class"))
				System.out.println(jarEntry);
		}
	}

	public Object recoverFromMismatchedSet(IntStream input, RecognitionException e, BitSet follow)
			throws RecognitionException {
		throw e;
	}

	protected Object recoverFromMismatchedToken(IntStream input, int ttype, BitSet follow)
			throws RecognitionException {
		// if next token is what we are looking for then "delete" this token
		if (mismatchIsUnwantedToken(input, ttype))
			throw new UnwantedTokenException(ttype, input);

		// can't recover with single token deletion, try insertion
		if (mismatchIsMissingToken(input, follow)) {
			Object inserted = getMissingSymbol(input, null, ttype, follow);
			throw new MissingTokenException(ttype, input, inserted);
		}

		throw new MismatchedTokenException(ttype, input);
	}

	protected abstract void parseSinks() throws RecognitionException;

	public SopremoPlan parse() throws RecognitionException {
		parseSinks();
		return new SopremoPlan(sinks);
	}

	protected Number parseInt(String text) {
		BigInteger result = new BigInteger(text);
		if (result.bitLength() <= 31)
			return result.intValue();
		if (result.bitLength() <= 63)
			return result.longValue();
		return result;
	}
}
