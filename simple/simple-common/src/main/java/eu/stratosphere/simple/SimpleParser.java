package eu.stratosphere.simple;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.antlr.runtime.BitSet;
import org.antlr.runtime.FailedPredicateException;
import org.antlr.runtime.IntStream;
import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.MissingTokenException;
import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.UnwantedTokenException;

import eu.stratosphere.sopremo.BuiltinProvider;
import eu.stratosphere.sopremo.ConstantRegistryCallback;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.ExpressionTagFactory;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.OperatorFactory;
import eu.stratosphere.sopremo.Bindings.BindingConstraint;
import eu.stratosphere.sopremo.OperatorInfo.OperatorPropertyInfo;
import eu.stratosphere.sopremo.OperatorInfo.PropertyInfo;
import eu.stratosphere.sopremo.OperatorInfo;
import eu.stratosphere.sopremo.Sink;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.sopremo.expressions.CoerceExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.MethodCall;
import eu.stratosphere.sopremo.function.Callable;
import eu.stratosphere.sopremo.function.Inlineable;
import eu.stratosphere.sopremo.function.JavaMethod;
import eu.stratosphere.sopremo.function.MacroBase;
import eu.stratosphere.sopremo.function.MethodRegistry;
import eu.stratosphere.sopremo.function.SopremoFunction;
import eu.stratosphere.sopremo.type.JsonNode;
import eu.stratosphere.util.InputSuggestion;
import eu.stratosphere.util.reflect.ReflectUtil;

public abstract class SimpleParser extends Parser {
	protected OperatorFactory operatorFactory = new OperatorFactory();

	private InputSuggestion<OperatorInfo<?>> operatorSuggestion;

	protected ExpressionTagFactory expressionTagFactory = new ExpressionTagFactory();

	protected List<Sink> sinks = new ArrayList<Sink>();

	private SopremoPlan currentPlan = new SopremoPlan();

	protected static enum ParserFlag {
		FUNCTION_OBJECTS;
	}

	private EnumSet<ParserFlag> flags = EnumSet.noneOf(ParserFlag.class);

	protected void addParserFlag(ParserFlag e) {
		this.flags.add(e);
	}

	protected void removeParserFlag(ParserFlag o) {
		this.flags.remove(o);
	}

	protected boolean hasParserFlag(ParserFlag o) {
		return this.flags.contains(o);
	}

	public SimpleParser(TokenStream input, RecognizerSharedState state) {
		super(input, state);
		this.importPackage("base");
	}

	public SimpleParser(TokenStream input) {
		super(input);
		this.importPackage("base");
	}

	public OperatorFactory getOperatorFactory() {
		return this.operatorFactory;
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

		this.operatorSuggestion = null;
	}

	private BindingConstraint[] bindingContraints;

	public <T> T getBinding(Token name, Class<T> expectedType) {
		try {
			return this.getContext().getBindings().get(name.getText(), expectedType, this.bindingContraints);
		} catch (Exception e) {
			throw new SimpleException(e.getMessage(), name);
		}
	}

	public boolean hasBinding(Token name, Class<?> expectedType) {
		try {
			Object result = this.getContext().getBindings().get(name.getText(), expectedType, this.bindingContraints);
			return result != null;
		} catch (Exception e) {
			return false;
		}
	}

	public <T> T getRawBinding(Token name, Class<T> expectedType) {
		try {
			return this.getContext().getBindings().get(name.getText(), expectedType);
		} catch (Exception e) {
			throw new SimpleException(e.getMessage(), name);
		}
	}

	public void addScope() {
		this.getContext().addScope();
	}

	protected EvaluationContext getContext() {
		return this.currentPlan.getContext();
	}

	public void removeScope() {
		this.getContext().removeScope();
	}

	public void setBinding(Token name, Object binding) {
		this.getContext().getBindings().set(name.getText(), binding);
	}

	public void setBinding(Token name, Object binding, int scopeLevel) {
		this.getContext().getBindings().set(name.getText(), binding, scopeLevel);
	}

	public EvaluationExpression createCheckedMethodCall(Token name, EvaluationExpression[] params) {
		return this.createCheckedMethodCall(name, null, params);
	}

	public EvaluationExpression createCheckedMethodCall(Token name, EvaluationExpression target,
			EvaluationExpression[] params) {
		Callable<?, ?> callable = this.getContext().getBindings().get(name.getText(), Callable.class);
		if (callable == null)
			throw new SimpleException("Unknown function", name);
		if (callable instanceof MacroBase)
			return ((MacroBase) callable).call(params, this.getContext());
		if (callable instanceof Inlineable)
			return ((Inlineable) callable).getDefinition();
		if (target != null) {
			ObjectArrayList<EvaluationExpression> paramList = ObjectArrayList.wrap(params);
			paramList.add(0, target);
			params = paramList.elements();
		}
		return new MethodCall(name.getText(), params);
	}

	// private Map<String, AtomicInteger> macroExpansionCounter = new HashMap<String, AtomicInteger>();
	//
	// /**
	// * @param macro
	// * @param params
	// * @return
	// */
	// private EvaluationExpression expandMacro(MacroBase macro, EvaluationExpression[] params) {
	// // AtomicInteger counter = macroExpansionCounter.get(macro.getName());
	// // if(counter == null)
	// // macroExpansionCounter.put(macro.getName(), counter = new AtomicInteger());
	// return macro.call(params, getContext());
	// }

	public <Op extends Operator<Op>> void setPropertySafely(OperatorInfo<Op> info, Operator<Op> op, String propertyName,
			Object value, Token reference) {
		OperatorPropertyInfo property = info.getOperatorProperty(propertyName);
		if (property == null)
			throw new SimpleException("Unknown property", reference);
		try {
			property.setValue(op, value);
		} catch (Exception e) {
			throw new SimpleException(String.format("Cannot set value of property %s to %s", propertyName, value),
				reference);
		}
	}

	public InputSuggestion<OperatorInfo<?>> getOperatorSuggestion() {
		if (this.operatorSuggestion == null)
			this.operatorSuggestion = new InputSuggestion<OperatorInfo<?>>(
				this.operatorFactory.getOperatorInfos()).
				withMaxSuggestions(3).
				withMinSimilarity(0.5);
		return this.operatorSuggestion;
	}
	
	public InputSuggestion<OperatorInfo.InputPropertyInfo> getInputPropertySuggestion(OperatorInfo<?> info) {
		return new InputSuggestion<OperatorInfo.InputPropertyInfo>(
				info.getInputProperties()).
				withMaxSuggestions(3).
				withMinSimilarity(0.5);
	}
	
	public InputSuggestion<OperatorInfo.OperatorPropertyInfo> getOperatorPropertySuggestion(OperatorInfo<?> info) {
		return new InputSuggestion<OperatorInfo.OperatorPropertyInfo>(
				info.getOperatorProperties()).
				withMaxSuggestions(3).
				withMinSimilarity(0.5);
	}

	private Map<String, Class<? extends JsonNode>> typeNameToType = new HashMap<String, Class<? extends JsonNode>>();

	public void addTypeAlias(String alias, Class<? extends JsonNode> type) {
		this.typeNameToType.put(alias, type);
	}

	public EvaluationExpression coerce(String type, EvaluationExpression valueExpression) {
		Class<? extends JsonNode> targetType = this.typeNameToType.get(type);
		if (targetType == null)
			throw new IllegalArgumentException("unknown type " + type);
		return new CoerceExpression(targetType, valueExpression);
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
			} else if (BuiltinProvider.class.isAssignableFrom(clazz))
				this.addFunctionsAndConstants(clazz);
		} catch (ClassNotFoundException e) {
			SimpleUtil.LOG.warn("could not load operator " + className);
		}
	}

	private void addFunctionsAndConstants(Class<?> clazz) {
		this.getContext().getFunctionRegistry().register(clazz);
		if (ConstantRegistryCallback.class.isAssignableFrom(clazz))
			((ConstantRegistryCallback) ReflectUtil.newInstance(clazz)).registerConstants(this.getContext());
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

	public OperatorInfo<?> findOperatorGreedily(Token firstWord) throws FailedPredicateException {
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
		OperatorInfo<?> info = null;
		for (; info == null && tokenCount > 0;)
			info = this.operatorFactory.getOperatorInfo(name.substring(0, wordBoundaries.getInt(--tokenCount)));

		// consume additional tokens
		for (; tokenCount > 0; tokenCount--)
			this.input.consume();

		if (info == null)
			throw new FailedPredicateException();
			/*throw new SimpleException(String.format("Unknown operator %s; possible alternatives %s", name,
				this.getOperatorSuggestion().suggest(name)), firstWord);*/

		return info;
	}

	public OperatorInfo.OperatorPropertyInfo findOperatorPropertyRelunctantly(OperatorInfo<?> info, Token firstWord) throws FailedPredicateException {
		String name = firstWord.getText();
		OperatorInfo.OperatorPropertyInfo property;
		
		int lookAhead = 1;
		// relunctantly concatenate tokens
		for (; (property = info.getOperatorProperty(name)) == null && this.input.LA(lookAhead) == firstWord.getType(); lookAhead++) {
			Token matchedToken = this.input.LT(lookAhead);
			name = String.format("%s %s", name, matchedToken.getText());
		}

		if (property == null)
//			return null;
//			throw new FailedPredicateException();
			throw new SimpleException(String.format("Unknown property %s; possible alternatives %s", name,
				this.getOperatorPropertySuggestion(info).suggest(name)), firstWord);

		// consume additional tokens
		for (; lookAhead > 1; lookAhead--)
			this.input.consume();
		
		return property;
	}

	public OperatorInfo.InputPropertyInfo findInputPropertyRelunctantly(OperatorInfo<?> info, Token firstWord) throws FailedPredicateException {
		String name = firstWord.getText();
		OperatorInfo.InputPropertyInfo property;
		
		int lookAhead = 1;
		// relunctantly concatenate tokens
		for (; (property = info.getInputPropertyInfo(name)) == null && this.input.LA(lookAhead) == firstWord.getType(); lookAhead++) {
			Token matchedToken = this.input.LT(lookAhead);
			name = String.format("%s %s", name, matchedToken.getText());
		}

		if (property == null)
			return null;
//			throw new FailedPredicateException();
//			throw new SimpleException(String.format("Unknown property %s; possible alternatives %s", name,
//				this.getOperatorPropertySuggestion(info).suggest(name)), firstWord);

		// consume additional tokens
		for (; lookAhead > 1; lookAhead--)
			this.input.consume();
		
		return property;
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

	public SopremoPlan parse() throws SimpleException {
		this.currentPlan = new SopremoPlan();
		try {
			this.setupParser();
			this.parseSinks();
		} catch (RecognitionException e) {
			throw new SimpleException("Cannot parse script", e);
		}
		this.currentPlan.setSinks(this.sinks);
		return this.currentPlan;
	}

	/**
	 * 
	 */
	protected void setupParser() {
		if (this.hasParserFlag(ParserFlag.FUNCTION_OBJECTS))
			this.bindingContraints = new BindingConstraint[] { BindingConstraint.AUTO_FUNCTION_POINTER,
				BindingConstraint.NON_NULL };
		else
			this.bindingContraints = new BindingConstraint[] { BindingConstraint.NON_NULL };
	}

	public void addFunction(SopremoFunction function) {
		this.getContext().getFunctionRegistry().register(function);
	}

	public void addFunction(String name, String udfPath) {
		int delim = udfPath.lastIndexOf('.');
		if (delim == -1)
			throw new IllegalArgumentException("Invalid path");
		String className = udfPath.substring(0, delim), methodName = udfPath.substring(delim + 1);

		JavaMethod function = new JavaMethod(name);
		try {
			Class<?> clazz = Class.forName(className);
			List<Method> functions = MethodRegistry.getCompatibleMethods(
				ReflectUtil.getMethods(clazz, methodName, Modifier.STATIC | Modifier.PUBLIC));
			for (Method method : functions)
				function.addSignature(method);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Unknown class " + className);
		}
		if (function.getSignatures().isEmpty())
			throw new IllegalArgumentException(String.format("Method %s not found in class %s", methodName, className));
		this.getContext().getFunctionRegistry().register(function);
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
