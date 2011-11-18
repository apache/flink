package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.JsonNode;

@OptimizerHints(scope = { Scope.OBJECT, Scope.ARRAY })
public class PathExpression extends ContainerExpression implements Cloneable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4663949354781572815L;

	private LinkedList<EvaluationExpression> fragments = new LinkedList<EvaluationExpression>();

	public PathExpression(final EvaluationExpression... fragments) {
		this(Arrays.asList(fragments));
	}

	public PathExpression(final List<? extends EvaluationExpression> fragments) {
		for (final EvaluationExpression evaluableExpression : fragments)
			if (evaluableExpression instanceof PathExpression)
				this.fragments.addAll(((PathExpression) evaluableExpression).fragments);
			else
				this.fragments.add(evaluableExpression);
	}

	public void add(final EvaluationExpression fragment) {
		this.fragments.add(fragment);
	}

	@Override
	public PathExpression clone() {
		return new PathExpression(fragments);
	}
	
	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final PathExpression other = (PathExpression) obj;
		return this.fragments.equals(other.fragments);
	}

	@Override
	public JsonNode evaluate(final JsonNode node, final EvaluationContext context) {
		JsonNode fragmentNode = node;
		for (final EvaluationExpression fragment : this.fragments)
			fragmentNode = fragment.evaluate(fragmentNode, context);
		return fragmentNode;
	}

	public int getDepth() {
		return this.fragments.size();
	}

	public EvaluationExpression getFragment(final int index) {
		return this.fragments.get(index);
	}

	public List<EvaluationExpression> getFragments() {
		return this.fragments;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.fragments.hashCode();
		return result;
	}

	public boolean isPrefix(final PathExpression prefix) {
		if (this.fragments.size() < prefix.getDepth())
			return false;
		return this.fragments.subList(0, prefix.getDepth()).equals(prefix.fragments);
	}

	@Override
	public Iterator<EvaluationExpression> iterator() {
		return this.fragments.iterator();
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ContainerExpression#getChildren()
	 */
	@Override
	public List<EvaluationExpression> getChildren() {
		return Collections.unmodifiableList(this.fragments);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ContainerExpression#setChildren(java.util.List)
	 */
	@Override
	public void setChildren(List<EvaluationExpression> children) {
		this.fragments.clear();
		this.fragments.addAll(children);
	}

	@Override
	public void replace(final EvaluationExpression toReplace, final EvaluationExpression replaceFragment) {
		super.replace(toReplace, replaceFragment);

		final PathExpression pathToFind = this.wrapAsPath(toReplace);
		final PathExpression replacePath = this.wrapAsPath(replaceFragment);
		int size = this.fragments.size() - pathToFind.fragments.size() + 1;
		final int findSize = pathToFind.fragments.size();
		findStartIndex: for (int startIndex = 0; startIndex < size; startIndex++) {
			for (int index = 0; index < findSize; index++)
				if (!this.fragments.get(startIndex + index).equals(pathToFind.fragments.get(index)))
					continue findStartIndex;

			this.fragments.subList(startIndex, startIndex + findSize).clear();
			this.fragments.addAll(startIndex, replacePath.fragments);
			size -= findSize - replacePath.fragments.size();
			// startIndex += replacePath.fragments.size();
		}
	}

	@Override
	public void toString(final StringBuilder builder) {
		for (final EvaluationExpression fragment : this.fragments) {
			fragment.toString(builder);
			builder.append(" ");
		}
	}

	private PathExpression wrapAsPath(final EvaluationExpression expression) {
		if (expression instanceof PathExpression)
			return (PathExpression) expression;
		return new PathExpression(expression);
	}

	public static EvaluationExpression valueOf(List<EvaluationExpression> expressions) {
		switch (expressions.size()) {
		case 0:
			return EvaluationExpression.VALUE;

		case 1:
			return expressions.get(0);

		default:
			return new PathExpression(expressions);
		}
	}

	public static EvaluationExpression valueOf(EvaluationExpression... expressions) {
		return valueOf(Arrays.asList(expressions));
	}

	@Override
	public JsonNode set(final JsonNode node, final JsonNode value, final EvaluationContext context) {
		JsonNode fragmentNode = node;
		final List<EvaluationExpression> fragments = this.getFragments();
		for (int index = 0; index < fragments.size() - 1; index++)
			fragmentNode = fragments.get(index).evaluate(fragmentNode, context);
		fragments.get(fragments.size() - 1).set(node, value, context);
		return node;
	}
	//
	// public static class Writable extends PathExpression implements WritableEvaluable {
	//
	// /**
	// *
	// */
	// private static final long serialVersionUID = 2014987314121118540L;
	//
	// public <T extends EvaluationExpression & WritableEvaluable> Writable(final List<T> fragments) {
	// super(fragments);
	// }
	//
	// public <T extends EvaluationExpression & WritableEvaluable> Writable(final T... fragments) {
	// super(fragments);
	// }
	//
	// @Override
	// public void add(final EvaluationExpression fragment) {
	// if (!(fragment instanceof WritableEvaluable))
	// throw new IllegalArgumentException();
	// super.add(fragment);
	// }
	//
	// @Override
	// public EvaluationExpression asExpression() {
	// return this;
	// }
	//
	// }

	/**
	 * 
	 */
	public void removeLast() {
		fragments.removeLast();
	}

	public void add(int index, EvaluationExpression fragment) {
		fragments.add(index, fragment);
	}
}