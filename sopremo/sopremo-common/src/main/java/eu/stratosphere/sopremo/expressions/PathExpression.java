package eu.stratosphere.sopremo.expressions;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Represents a chain of {@link EvaluationExpression}s where the result of one expression serves as the input for the
 * next one.
 */
@OptimizerHints(scope = { Scope.OBJECT, Scope.ARRAY })
public class PathExpression extends ContainerExpression implements Cloneable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4663949354781572815L;

	private LinkedList<EvaluationExpression> fragments;

	public PathExpression(final EvaluationExpression... fragments) {
		this(Arrays.asList(fragments));
	}

	public PathExpression(final List<? extends EvaluationExpression> fragments) {
		this(normalize(fragments));
	}

	private PathExpression(final LinkedList<EvaluationExpression> fragments) {
		this.fragments = fragments;
	}

	public void add(final EvaluationExpression fragment) {
		this.fragments.add(fragment);
	}

	@Override
	public PathExpression clone() {
		return new PathExpression(this.fragments);
	}

	@Override
	public boolean equals(final Object obj) {
		if (!super.equals(obj))
			return false;
		final PathExpression other = (PathExpression) obj;
		return this.fragments.equals(other.fragments);
	}

	@Override
	public IJsonNode evaluate(final IJsonNode node, IJsonNode target, final EvaluationContext context) {
		IJsonNode fragmentNode = node;
		for (final EvaluationExpression fragment : this.fragments)
			fragmentNode = fragment.evaluate(fragmentNode, null, context);
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
		int result = super.hashCode();
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

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ContainerExpression#getChildren()
	 */
	@Override
	public List<EvaluationExpression> getChildren() {
		return Collections.unmodifiableList(this.fragments);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.ContainerExpression#setChildren(java.util.List)
	 */
	@Override
	public void setChildren(final List<? extends EvaluationExpression> children) {
		this.fragments = normalize(children);
	}

	@Override
	public void replace(final EvaluationExpression toReplace, final EvaluationExpression replaceFragment) {
		super.replace(toReplace, replaceFragment);

		final PathExpression pathToFind = PathExpression.ensurePathExpression(toReplace);
		final PathExpression replacePath = PathExpression.ensurePathExpression(replaceFragment);
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

	public static PathExpression ensurePathExpression(final EvaluationExpression expression) {
		if (expression instanceof PathExpression)
			return (PathExpression) expression;
		return new PathExpression(expression);
	}

	private static LinkedList<EvaluationExpression> normalize(final List<? extends EvaluationExpression> fragments) {
		final LinkedList<EvaluationExpression> linkedList = new LinkedList<EvaluationExpression>();

		for (final EvaluationExpression fragment : fragments)
			if (fragment instanceof PathExpression)
				linkedList.addAll(((PathExpression) fragment).fragments);
			else if (!canIgnore(fragment))
				linkedList.add(fragment);

		return linkedList;
	}

	public static EvaluationExpression wrapIfNecessary(final List<EvaluationExpression> expressions) {
		final LinkedList<EvaluationExpression> normalized = normalize(expressions);
		switch (normalized.size()) {
		case 0:
			return EvaluationExpression.VALUE;

		case 1:
			return expressions.get(0);

		default:
			return new PathExpression(normalized);
		}
	}

	public static EvaluationExpression wrapIfNecessary(final EvaluationExpression... expressions) {
		return wrapIfNecessary(Arrays.asList(expressions));
	}

	@Override
	public IJsonNode set(final IJsonNode node, final IJsonNode value, final EvaluationContext context) {
		IJsonNode fragmentNode = node;
		final List<EvaluationExpression> fragments = this.getFragments();
		for (int index = 0; index < fragments.size() - 1; index++)
			fragmentNode = fragments.get(index).evaluate(fragmentNode, null, context);
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
		this.fragments.removeLast();
	}

	public void add(final int index, final EvaluationExpression fragment) {
		if (!canIgnore(fragment))
			this.fragments.add(index, fragment);
	}

	private static boolean canIgnore(final EvaluationExpression fragment) {
		return fragment == EvaluationExpression.VALUE;
	}

	public PathExpression subPath(int start, int end) {
		if (start < 0)
			start = this.fragments.size() + 1 + start;
		if (end < 0)
			end = this.fragments.size() + 1 + end;
		return new PathExpression(this.fragments.subList(start, end));
	}
}