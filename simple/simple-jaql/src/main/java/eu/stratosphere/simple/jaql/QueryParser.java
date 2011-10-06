package eu.stratosphere.simple.jaql;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.antlr.runtime.ANTLRInputStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

import eu.stratosphere.simple.PlanCreator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.OperatorFactory.OperatorInfo;
import eu.stratosphere.sopremo.SopremoPlan;
import eu.stratosphere.util.StringUtil;

public class QueryParser extends PlanCreator {

	Deque<List<Operator<?>>> operatorInputs = new LinkedList<List<Operator<?>>>();

	@Override
	public SopremoPlan getPlan(InputStream stream) {
		try {
			return tryParse(stream);
		} catch (Exception e) {
			return null;
		}
	}

	public SopremoPlan tryParse(InputStream stream) throws IOException, RecognitionException {
		SJaqlLexer lexer = new SJaqlLexer(new ANTLRInputStream(stream));
		CommonTokenStream tokens = new CommonTokenStream();
		tokens.setTokenSource(lexer);
		SJaqlParser parser = new SJaqlParser(tokens);
		parser.setTreeAdaptor(new SopremoTreeAdaptor());
		return parser.parse();
	}

	public String toJavaString(InputStream stream) throws IOException, RecognitionException {
		SJaqlLexer lexer = new SJaqlLexer(new ANTLRInputStream(stream));
		CommonTokenStream tokens = new CommonTokenStream();
		tokens.setTokenSource(lexer);
		SJaqlParser parser = new SJaqlParser(tokens);
		TraceableSopremoTreeAdaptor adaptor = new TraceableSopremoTreeAdaptor();
		parser.setTreeAdaptor(adaptor);
		SopremoPlan result = parser.parse();
		JavaRenderInfo info = new JavaRenderInfo(parser, adaptor);
		toJavaString(result, info);
		return info.builder.toString();
	}

	protected String toJavaString(SopremoPlan result, JavaRenderInfo info) {
		for (Operator<?> op : result.getContainedOperators())
			appendJavaOperator(op, info);
		return info.builder.toString();
	}

	protected <O extends Operator<O>> void appendJavaOperator(Operator<O> op, JavaRenderInfo renderInfo) {
		String className = op.getClass().getSimpleName();
		renderInfo.builder.append(String.format("%s %s = new %1$s();\n", className, renderInfo.getVariableName(op)));

		@SuppressWarnings("unchecked")
		OperatorInfo<O> info = renderInfo.parser.getOperatorFactory().getOperatorInfo((Class<O>) op.getClass());
		Operator<O> defaultInstance = info.newInstance();
		appendInputs(op, renderInfo, defaultInstance);
		defaultInstance.setInputs(op.getInputs());
		appendOperatorProperties(op, renderInfo, info, defaultInstance);
		appendInputProperties(op, renderInfo, info, defaultInstance);
	}

	protected <O extends Operator<O>> void appendInputs(Operator<O> op, JavaRenderInfo renderInfo,
			Operator<O> defaultInstance) {
		if (!defaultInstance.getInputs().equals(op.getInputs())) {
			renderInfo.builder.append(renderInfo.getVariableName(op)).append(".setInputs(");
			for (int index = 0; index < op.getInputs().size(); index++) {
				if (index > 0)
					renderInfo.builder.append(", ");
				renderInfo.builder.append(renderInfo.getVariableName(op.getInput(index)));
			}
			renderInfo.builder.append(");\n");
		}
	}

	protected <O extends Operator<O>> void appendInputProperties(Operator<O> op, JavaRenderInfo renderInfo,
			OperatorInfo<O> info, Operator<O> defaultInstance) {
		for (Entry<String, PropertyDescriptor> property : info.getInputProperties().entrySet()) {
			for (int index = 0; index < op.getInputs().size(); index++) {
				String propertyName = property.getKey();
				Object actualValue = info.getInputProperty(propertyName, op, index);
				Object defaultValue = info.getInputProperty(propertyName, defaultInstance, index);
				if (!actualValue.equals(defaultValue))
					renderInfo.builder.append(String.format("%s.set%s(%d, %s);\n", renderInfo.getVariableName(op),
						StringUtil.upperFirstChar(property.getValue().getName()), index, actualValue));
			}
		}
	}

	protected <O extends Operator<O>> void appendOperatorProperties(Operator<O> op, JavaRenderInfo renderInfo,
			OperatorInfo<O> info,
			Operator<O> defaultInstance) {
		for (Entry<String, PropertyDescriptor> property : info.getOperatorProperties().entrySet()) {
			String propertyName = property.getKey();
			Object actualValue = info.getProperty(propertyName, op);
			Object defaultValue = info.getProperty(propertyName, defaultInstance);
			if (!actualValue.equals(defaultValue))
				renderInfo.builder.append(String.format("%s.set%s(%s);\n", renderInfo.getVariableName(op),
					StringUtil.upperFirstChar(property.getValue().getName()), actualValue));
		}
	}

	private static class JavaRenderInfo {
		private SJaqlParser parser;

		private TraceableSopremoTreeAdaptor adaptor;

		private StringBuilder builder = new StringBuilder();

		private Map<JsonStream, String> variableNames = new IdentityHashMap<JsonStream, String>();

		public JavaRenderInfo(SJaqlParser parser, TraceableSopremoTreeAdaptor adaptor) {
			this.parser = parser;
			this.adaptor = adaptor;
		}

		public String getVariableName(JsonStream input) {
			Operator<?> op = (input instanceof Operator ? (Operator<?>) input : input.getSource().getOperator());
			String name = variableNames.get(op);
			if (name == null) {
				int counter = instanceCounter.getInt(op.getClass()) + 1;
				instanceCounter.put(op.getClass(), counter);
				name = String.format("%s%d", StringUtil.lowerFirstChar(op.getClass().getSimpleName()), counter);
				variableNames.put(op, name);
			}
			return name;
		}

		private Object2IntMap<Class<?>> instanceCounter = new Object2IntOpenHashMap<Class<?>>();
	}
}
