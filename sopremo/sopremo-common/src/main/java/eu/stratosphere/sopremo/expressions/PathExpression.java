package eu.stratosphere.sopremo.expressions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.util.CollectionUtil;

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

	private transient List<IJsonNode> fragmentTargets = new ArrayList<IJsonNode>();

	/**
	 * Initializes a PathExpression with the given {@link EvaluationExpression}s.
	 * 
	 * @param fragments
	 *        an Array of the expressions that should be used
	 */
	public PathExpression(final EvaluationExpression... fragments) {
		this(Arrays.asList(fragments));
	}

	/**
	 * Initializes a PathExpression with the given {@link EvaluationExpression}s.
	 * 
	 * @param fragments
	 *        a List of the expressions that should be used
	 */
	public PathExpression(final List<? extends EvaluationExpression> fragments) {
		this(normalize(fragments));
	}

	private PathExpression(final LinkedList<EvaluationExpression> fragments) {
		this.fragments = fragments;
		CollectionUtil.ensureSize(this.fragmentTargets, this.fragments.size());
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		this.fragmentTargets = new ArrayList<IJsonNode>();
		CollectionUtil.ensureSize(this.fragmentTargets, this.fragments.size());
	}

	/**
	 * Adds a new {@link EvaluationExpression} to the end of the path
	 * 
	 * @param fragment
	 *        the expression that should be added
	 */
	public void add(final EvaluationExpression fragment) {
		this.fragments.add(fragment);
		this.fragmentTargets.add(null);
	}

	@Override
	public PathExpression clone() {
		final PathExpression klone = (PathExpression) super.clone();
		klone.fragments = new LinkedList<EvaluationExpression>(this.fragments);
		final ListIterator<EvaluationExpression> cloneIterator = klone.fragments.listIterator();
		while (cloneIterator.hasNext())
			cloneIterator.set(cloneIterator.next().clone());
		return klone;
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
		for (int index = 0; index < this.fragments.size(); index++)
			this.fragmentTargets.set(index,
				fragmentNode =
					this.fragments.get(index).evaluate(fragmentNode, this.fragmentTargets.get(index), context));
		return fragmentNode;
	}

	/**
	 * Returns the lenght of the path
	 * 
	 * @return the lenght
	 */
	public int getDepth() {
		return this.fragments.size();
	}

	/**
	 * Returns the {@link EvaluationExpression} at the specified index
	 * 
	 * @param index
	 *        the index of the expression that should be returned
	 * @return the expression at the specified index
	 */
	public EvaluationExpression getFragment(final int index) {
		return this.fragments.get(index);
	}

	/**
	 * Returns the last {@link EvaluationExpression}.
	 * 
	 * @return the last expression
	 */
	public EvaluationExpression getLastFragment() {
		return this.fragments.getLast();
	}

	/**
	 * Returns all expressions
	 * 
	 * @return the expressions
	 */
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

	/**
	 * Returns either the given {@link PathExpression} is a prefix of this expression or not
	 * 
	 * @param prefix
	 *        the expression that should be checked
	 * @return the given expression is a prefix or not
	 */
	public boolean isPrefix(final PathExpression prefix) {
		if (this.fragments.size() < prefix.getDepth())
			return false;
		return this.fragments.subList(0, prefix.getDepth()).equals(prefix.fragments);
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

	// @Override
	// public void replace(final EvaluationExpression toReplace, final EvaluationExpression replaceFragment) {
	// super.replace(toReplace, replaceFragment);
	//
	// final PathExpression pathToFind = PathExpression.ensurePathExpression(toReplace);
	// final PathExpression replacePath = PathExpression.ensurePathExpression(replaceFragment);
	// int size = this.fragments.size() - pathToFind.fragments.size() + 1;
	// final int findSize = pathToFind.fragments.size();
	// findStartIndex: for (int startIndex = 0; startIndex < size; startIndex++) {
	// for (int index = 0; index < findSize; index++)
	// if (!this.fragments.get(startIndex + index).equals(pathToFind.fragments.get(index)))
	// continue findStartIndex;
	//
	// this.fragments.subList(startIndex, startIndex + findSize).clear();
	// this.fragments.addAll(startIndex, replacePath.fragments);
	// size -= findSize - replacePath.fragments.size();
	// // startIndex += replacePath.fragments.size();
	// }
	// }

	@Override
	public void toString(final StringBuilder builder) {
		for (final EvaluationExpression fragment : this.fragments) {
			fragment.toString(builder);
			builder.append(" ");
		}
	}

	/**
	 * Creates a new {@link PathExpression} with the given expression
	 * 
	 * @param expression
	 *        the expression that should be used for the new {@link PathExpression}
	 * @return the new {@link PathExpression}
	 */
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

	/**
	 * Wraps the given {@link EvaluationExpression}s in a single {@link PathExpression}
	 * 
	 * @param expressions
	 *        a List of the expressions that should be wrapped
	 * @return the {@link PathExpression}
	 */
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

	/**
	 * Wraps the given {@link EvaluationExpression}s in a single {@link PathExpression}
	 * 
	 * @param expressions
	 *        an Array of the expressions that should be wrapped
	 * @return the {@link PathExpression}
	 */
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
	 * Removes the last expression in this path.
	 */
	public void removeLast() {
		this.fragments.removeLast();
	}

	/**
	 * Adds the given {@link EvaluationExpression} at the specified index into the path.
	 * 
	 * @param index
	 *        the index where the expression should be added
	 * @param fragment
	 *        the expression that should be added
	 */
	public void add(final int index, final EvaluationExpression fragment) {
		if (!canIgnore(fragment))
			this.fragments.add(index, fragment);
	}

	private static boolean canIgnore(final EvaluationExpression fragment) {
		return fragment == EvaluationExpression.VALUE;
	}

	/**
	 * Creates a {@link PathExpression} which represents a sub path from this expressions path.
	 * 
	 * @param start
	 *        the startindex of the sub path (including)
	 * @param end
	 *        the endindex of teh sub path (excluding)
	 * @return the created {@link PathExpression}
	 */
	public PathExpression subPath(int start, int end) {
		if (start < 0)
			start = this.fragments.size() + 1 + start;
		if (end < 0)
			end = this.fragments.size() + 1 + end;
		return new PathExpression(this.fragments.subList(start, end));
	}
}