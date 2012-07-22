package eu.stratosphere.sopremo.query;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.CoerceExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ExpressionTagFactory;
import eu.stratosphere.sopremo.expressions.MethodCall;
import eu.stratosphere.sopremo.function.Callable;
import eu.stratosphere.sopremo.function.ExpressionFunction;
import eu.stratosphere.sopremo.function.Inlineable;
import eu.stratosphere.sopremo.function.JavaMethod;
import eu.stratosphere.sopremo.function.MacroBase;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.packages.AbstractMethodRegistry;
import eu.stratosphere.sopremo.packages.IConstantRegistry;
import eu.stratosphere.sopremo.packages.IMethodRegistry;
import eu.stratosphere.sopremo.query.Bindings.BindingConstraint;
import eu.stratosphere.sopremo.query.OperatorInfo.OperatorPropertyInfo;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.util.InputSuggestion;
import eu.stratosphere.util.reflect.ReflectUtil;

public abstract class AbstractQueryParser extends Parser {
	private PackageManager packageManager = new PackageManager();
	
	private InputSuggestion<OperatorInfo<?>> operatorSuggestion;

	private ExpressionTagFactory expressionTagFactory = new ExpressionTagFactory();

	private List<Sink> sinks = new ArrayList<Sink>();

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

	public AbstractQueryParser(TokenStream input, RecognizerSharedState state) {
		super(input, state);
		packageManager.importPackage("base");
	}

	public AbstractQueryParser(TokenStream input) {
		super(input);
		packageManager.importPackage("base");
	}


	
	
	public IOperatorRegistry getOperatorRegistry() {
		return this.packageManager.getOperatorRegistry();
	}

	public IConstantRegistry getConstantRegistry() {
		return this.packageManager.getConstantRegistry();
	}

	public IMethodRegistry getMethodRegistry() {
		return this.packageManager.getMethodRegistry();
	}




	private BindingConstraint[] bindingContraints;

	public <T> T getBinding(Token name, Class<T> expectedType) {
		try {
			return this.getContext().getBindings().get(name.getText(), expectedType, this.bindingContraints);
		} catch (Exception e) {
			throw new QueryParserException(e.getMessage(), name);
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
			throw new QueryParserException(e.getMessage(), name);
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
			throw new QueryParserException("Unknown function", name);
		if (callable instanceof MacroBase)
			return ((MacroBase) callable).call(params, null, this.getContext());
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
			throw new QueryParserException("Unknown property", reference);
		try {
			property.setValue(op, value);
		} catch (Exception e) {
			throw new QueryParserException(String.format("Cannot set value of property %s to %s", propertyName, value),
				reference);
		}
	}

	public InputSuggestion<OperatorInfo<?>> getOperatorSuggestion() {
		if (this.operatorSuggestion == null)
			this.operatorSuggestion = new InputSuggestion<OperatorInfo<?>>(
				getOperatorFactory().getOperatorInfos()).
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

	private Map<String, Class<? extends IJsonNode>> typeNameToType = new HashMap<String, Class<? extends IJsonNode>>();

	public void addTypeAlias(String alias, Class<? extends IJsonNode> type) {
		this.typeNameToType.put(alias, type);
	}

	public EvaluationExpression coerce(String type, EvaluationExpression valueExpression) {
		Class<? extends IJsonNode> targetType = this.typeNameToType.get(type);
		if (targetType == null)
			throw new IllegalArgumentException("unknown type " + type);
		return new CoerceExpression(targetType, valueExpression);
	}
	@Override
	public Object recoverFromMismatchedSet(IntStream input, RecognitionException e, BitSet follow)
			throws RecognitionException {
		throw e;
	}

	public OperatorInfo<?> findOperatorGreedily(String prefix, Token firstWord) throws FailedPredicateException {
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
			info = getOperatorFactory().getOperatorInfo(prefix, name.substring(0, wordBoundaries.getInt(--tokenCount)));

		// consume additional tokens
		for (; tokenCount > 0; tokenCount--)
			this.input.consume();

		if (info == null)
			throw new FailedPredicateException(firstWord.getInputStream(), "operator name", String.format("Unknown operator %s; possible alternatives %s", name,
				this.getOperatorSuggestion().suggest(name)));
			/*throw new SimpleException(String.format("Unknown operator %s; possible alternatives %s", name,
				this.getOperatorSuggestion().suggest(name)), firstWord);*/

		return info;
	}

	public OperatorInfo.OperatorPropertyInfo findOperatorPropertyRelunctantly(OperatorInfo<?> info, Token firstWord) throws FailedPredicateException {
		String name = firstWord.getText();
		OperatorInfo.OperatorPropertyInfo property;
		
		int lookAhead = 1;
		// Reluctantly concatenate tokens
		for (; (property = info.getOperatorProperty(name)) == null && this.input.LA(lookAhead) == firstWord.getType(); lookAhead++) {
			Token matchedToken = this.input.LT(lookAhead);
			name = String.format("%s %s", name, matchedToken.getText());
		}

		if (property == null)
//			return null;
//			throw new FailedPredicateException();
			throw new QueryParserException(String.format("Unknown property %s; possible alternatives %s", name,
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
		// Reluctantly concatenate tokens
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

	public SopremoPlan parse() throws QueryParserException {
		this.currentPlan = new SopremoPlan();
		try {
			this.setupParser();
			this.parseSinks();
		} catch (RecognitionException e) {
			throw new QueryParserException("Cannot parse script", e);
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

	public void addFunction(ExpressionFunction function) {
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
			List<Method> functions = AbstractMethodRegistry.getCompatibleMethods(
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
