package eu.stratosphere.simple.jaql;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.BaseTreeAdaptor;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;

import eu.stratosphere.pact.common.util.ReflectionUtil;
import eu.stratosphere.sopremo.ExpressionFactory;
import eu.stratosphere.sopremo.expressions.ErroneousExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.util.reflect.ReflectUtil;

public class SopremoTreeAdaptor extends BaseTreeAdaptor implements TreeAdaptor {
	private ExpressionFactory expressionFactory = new ExpressionFactory();

	public class PlaceholderExpression extends ErroneousExpression {
		private List<Object> params = new ArrayList<Object>();

		private Class<?> expressionClass;

		public PlaceholderExpression(Class<?> clazz) {
			super("<empty>");
			this.expressionClass = clazz;
		}

		public PlaceholderExpression() {
			super("<empty>");
		}

		public List<Object> getParams() {
			return params;
		}

		public Class<?> getExpressionClass() {
			return expressionClass;
		}

		@Override
		protected void toString(StringBuilder builder) {
			if (expressionClass != null)
				builder.append(expressionClass.getSimpleName());
			else
				builder.append("<unknown>");
			builder.append('(').append(params).append(')');
		}
	}

	@Override
	public Object create(int tokenType, String text) {
		assert tokenType == SJaqlLexer.EXPRESSION : "token type not supported";
		return new PlaceholderExpression(expressionFactory.getExpressionType(text));
	}

	@Override
	public Object becomeRoot(Object newRoot, Object oldRoot) {
		return newRoot;
	}

	@Override
	public Object becomeRoot(Token newRoot, Object oldRoot) {
		return super.becomeRoot(newRoot, oldRoot);
	}

	@Override
	public Object create(int tokenType, Token fromToken) {
		assert tokenType == SJaqlLexer.EXPRESSION : "token type not supported";
		return new PlaceholderExpression();
	}

	@Override
	public void addChild(Object t, Object child) {
		if (child != null)
			((PlaceholderExpression) t).getParams().add(child);
	}

	@Override
	public Object rulePostProcessing(Object root) {
		if (root == null)
			return null;
		
		PlaceholderExpression placeholder = (PlaceholderExpression) root;
		
		if(placeholder.params.isEmpty())
			return placeholder;
		
		if(!(placeholder.params.get(0) instanceof PlaceholderExpression))
			return placeholder.params.get(0);
			
		placeholder = (PlaceholderExpression) placeholder.params.get(0);
		Class<?> expressionClass = placeholder.getExpressionClass();
		if (expressionClass == null)
			return null;
		assert expressionClass != null : "could not determine expression class";

		return ReflectUtil.newInstance(expressionClass, placeholder.params.toArray(new Object[0]));
	}

	@Override
	public Object nil() {
		return new PlaceholderExpression();
	}

	@Override
	public Object create(Token payload) {
		return create(payload.getType(), payload.getText());
	}

	@Override
	public Object dupNode(Object treeNode) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Token getToken(Object t) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTokenBoundaries(Object t, Token startToken, Token stopToken) {
	}

	@Override
	public int getTokenStartIndex(Object t) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getTokenStopIndex(Object t) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getParent(Object t) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setParent(Object t, Object parent) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getChildIndex(Object t) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setChildIndex(Object t, int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void replaceChildren(Object parent, int startChildIndex, int stopChildIndex, Object t) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Token createToken(int tokenType, String text) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Token createToken(Token fromToken) {
		throw new UnsupportedOperationException();
	}

}
