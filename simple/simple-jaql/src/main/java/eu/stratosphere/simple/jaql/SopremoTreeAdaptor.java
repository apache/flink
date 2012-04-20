package eu.stratosphere.simple.jaql;

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.BaseTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;

import eu.stratosphere.sopremo.ExpressionFactory;
import eu.stratosphere.sopremo.expressions.UnevaluableExpression;
import eu.stratosphere.util.reflect.ReflectUtil;

public class SopremoTreeAdaptor extends BaseTreeAdaptor implements TreeAdaptor {
	private final ExpressionFactory expressionFactory = new ExpressionFactory();

	@Override
	public void addChild(final Object t, final Object child) {
		if (child != null)
			((PlaceholderExpression) t).getParams().add(child);
	}

	@Override
	public Object becomeRoot(final Object newRoot, final Object oldRoot) {
		return newRoot;
	}

	@Override
	public Object becomeRoot(final Token newRoot, final Object oldRoot) {
		return super.becomeRoot(newRoot, oldRoot);
	}

	@Override
	public Object create(final int tokenType, final String text) {
		assert tokenType == SJaqlLexer.EXPRESSION : "token type not supported";
		return new PlaceholderExpression(this.expressionFactory.getExpressionType(text));
	}

	@Override
	public Object create(final int tokenType, final Token fromToken) {
		assert tokenType == SJaqlLexer.EXPRESSION : "token type not supported";
		return new PlaceholderExpression();
	}

	@Override
	public Object create(final Token payload) {
		return this.create(payload.getType(), payload.getText());
	}

	@Override
	public Token createToken(final int tokenType, final String text) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Token createToken(final Token fromToken) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object dupNode(final Object treeNode) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getChildIndex(final Object t) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getParent(final Object t) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Token getToken(final Object t) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getTokenStartIndex(final Object t) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getTokenStopIndex(final Object t) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object nil() {
		return new PlaceholderExpression();
	}

	@Override
	public void replaceChildren(final Object parent, final int startChildIndex, final int stopChildIndex, final Object t) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object rulePostProcessing(final Object root) {
		if (root == null)
			return null;

		PlaceholderExpression placeholder = (PlaceholderExpression) root;

		if (placeholder.params.isEmpty())
			return placeholder;

		if (!(placeholder.params.get(0) instanceof PlaceholderExpression))
			return placeholder.params.get(0);

		placeholder = (PlaceholderExpression) placeholder.params.get(0);
		final Class<?> expressionClass = placeholder.getExpressionClass();
		if (expressionClass == null)
			return null;
		assert expressionClass != null : "could not determine expression class";

		final Object[] params = placeholder.params.toArray(new Object[0]);
		return this.instantiate(expressionClass, params);
	}

	protected Object instantiate(final Class<?> expressionClass, final Object[] params) {
		return ReflectUtil.newInstance(expressionClass, params);
	}

	@Override
	public void setChildIndex(final Object t, final int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setParent(final Object t, final Object parent) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTokenBoundaries(final Object t, final Token startToken, final Token stopToken) {
	}

	public class PlaceholderExpression extends UnevaluableExpression {
		/**
		 * 
		 */
		private static final long serialVersionUID = 8956295880318403461L;

		private final List<Object> params = new ArrayList<Object>();

		private Class<?> expressionClass;

		public PlaceholderExpression() {
			super("<empty>");
		}

		public PlaceholderExpression(final Class<?> clazz) {
			super("<empty>");
			this.expressionClass = clazz;
		}

		public Class<?> getExpressionClass() {
			return this.expressionClass;
		}

		public List<Object> getParams() {
			return this.params;
		}

		@Override
		public void toString(final StringBuilder builder) {
			if (this.expressionClass != null)
				builder.append(this.expressionClass.getSimpleName());
			else
				builder.append("<unknown>");
			builder.append('(').append(this.params).append(')');
		}
	}

}
