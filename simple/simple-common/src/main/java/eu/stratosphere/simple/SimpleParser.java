package eu.stratosphere.simple;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

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
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.UnwantedTokenException;
import eu.stratosphere.sopremo.ExpressionTagFactory;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.OperatorFactory;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;

public abstract class SimpleParser extends Parser {
	protected OperatorFactory operatorFactory = new OperatorFactory();

	protected ExpressionTagFactory expressionTagFactory = new ExpressionTagFactory();

	protected List<Sink> sinks = new ArrayList<Sink>();

	public SimpleParser(TokenStream input, RecognizerSharedState state) {
		super(input, state);
		this.importPackage("base");
	}

	public SimpleParser(TokenStream input) {
		super(input);
		this.importPackage("base");
	}

	/**
	 * Imports sopremo-&lt;packageName&gt;.jar
	 * 
	 * @param packageName
	 */
	public void importPackage(String packageName) {
		String packagePath = this.getPackagePath(packageName);

		SimpleUtil.LOG.debug("adding package " + packagePath);
		try {
			if (packagePath.endsWith(".jar"))
				this.importFromJar(new File(packagePath).getAbsolutePath(), new File(packagePath));
			else
				// should only happen while debugging
				this.importFromProject(new File(packagePath).getAbsolutePath(), new File(packagePath));
		} catch (IOException e) {
			throw new IllegalArgumentException(String.format("could not load package %s", packagePath));
		}

	}

	protected String getPackagePath(String packageName) {
		String classpath = System.getProperty("java.class.path");
		String sopremoPackage = "sopremo-" + packageName;
		for (String path : classpath.split(File.pathSeparator))
			if (path.contains(sopremoPackage))
				return path;
		throw new IllegalArgumentException(String.format("no package %s found", sopremoPackage));
	}

	private void importFromProject(String classPath, File dir) {
		for (File file : dir.listFiles())
			if (file.isDirectory())
				this.importFromProject(classPath, file);
			else if (file.getName().endsWith(".class") && !file.getName().contains("$"))
				this.importFromFile(classPath, file);
	}

	@SuppressWarnings("unchecked")
	private void importFromFile(String classPath, File file) {
		String classFileName = file.getAbsolutePath().substring(classPath.length());
		String className = classFileName.replaceAll(".class$", "").replaceAll("/|\\\\", ".").replaceAll("^\\.", "");
		Class<?> clazz;
		try {
			clazz = Class.forName(className);
			if (Operator.class.isAssignableFrom(clazz) && (clazz.getModifiers() & Modifier.ABSTRACT) == 0) {
				SimpleUtil.LOG.trace("adding operator " + clazz);
				this.operatorFactory.addOperator((Class<? extends Operator<?>>) clazz);
			}
		} catch (ClassNotFoundException e) {
			SimpleUtil.LOG.warn("could not load operator " + className);
		}
	}

	@SuppressWarnings("unused")
	private void importFromJar(String classPath, File file) throws IOException {
		Enumeration<JarEntry> entries = new JarFile(file).entries();
		while (entries.hasMoreElements()) {
			JarEntry jarEntry = entries.nextElement();
			if (jarEntry.getName().endsWith(".class"))
				System.out.println(jarEntry);
		}
	}

	@Override
	public Object recoverFromMismatchedSet(IntStream input, RecognitionException e, BitSet follow)
			throws RecognitionException {
		throw e;
	}

	public OperatorFactory.OperatorInfo<?> findOperatorGreedily(Token firstWord) {
		StringBuilder name = new StringBuilder(firstWord.getText());
		IntList wordBoundaries = new IntArrayList();
		wordBoundaries.add(name.length());

		// greedily concatenate as many tokens as possible
		for (int lookAhead = 1; this.input.LA(lookAhead) == firstWord.getType(); lookAhead++) {
			Token matchedToken = this.input.LT(lookAhead);
			name.append(' ').append(matchedToken.getText());
			wordBoundaries.add(name.length());
		}

		int tokenCount = wordBoundaries.size();
		OperatorFactory.OperatorInfo<?> info = null;
		for (; info == null && tokenCount > 0; )
			info = this.operatorFactory.getOperatorInfo(name.substring(0, wordBoundaries.getInt(--tokenCount)));

		// consume additional tokens
		for (; tokenCount > 0; tokenCount--)
			this.input.consume();

		return info;
	}

	@Override
	protected Object recoverFromMismatchedToken(IntStream input, int ttype, BitSet follow)
			throws RecognitionException {
		// if next token is what we are looking for then "delete" this token
		if (this.mismatchIsUnwantedToken(input, ttype))
			throw new UnwantedTokenException(ttype, input);

		// can't recover with single token deletion, try insertion
		if (this.mismatchIsMissingToken(input, follow)) {
			Object inserted = this.getMissingSymbol(input, null, ttype, follow);
			throw new MissingTokenException(ttype, input, inserted);
		}

		throw new MismatchedTokenException(ttype, input);
	}

	protected abstract void parseSinks() throws RecognitionException;

	public SopremoPlan parse() throws RecognitionException {
		this.parseSinks();
		return new SopremoPlan(this.sinks);
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
