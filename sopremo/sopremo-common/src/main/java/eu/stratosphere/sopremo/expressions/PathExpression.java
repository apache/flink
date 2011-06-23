package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;

@OptimizerHints(scope = { Scope.OBJECT, Scope.ARRAY })
public class PathExpression extends ContainerExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4663949354781572815L;

	private List<EvaluableExpression> fragments = new ArrayList<EvaluableExpression>();

	public PathExpression(EvaluableExpression... fragments) {
		this(Arrays.asList(fragments));
	}

	public PathExpression(List<EvaluableExpression> fragments) {
		for (EvaluableExpression evaluableExpression : fragments)
			if (evaluableExpression instanceof PathExpression)
				this.fragments.addAll(((PathExpression) evaluableExpression).fragments);
			else
				this.fragments.add(evaluableExpression);
	}

	public void add(EvaluableExpression fragment) {
		this.fragments.add(fragment);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		PathExpression other = (PathExpression) obj;
		return this.fragments.equals(other.fragments);
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		JsonNode fragmentNode = node;
		for (Evaluable fragment : this.fragments)
			fragmentNode = fragment.evaluate(fragmentNode, context);
		return fragmentNode;
	}

	public int getDepth() {
		return this.fragments.size();
	}

	public EvaluableExpression getFragment(int index) {
		return this.fragments.get(index);
	}

	public List<EvaluableExpression> getFragments() {
		return this.fragments;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.fragments.hashCode();
		return result;
	}

	public boolean isPrefix(PathExpression prefix) {
		if (this.fragments.size() < prefix.getDepth())
			return false;
		return this.fragments.subList(0, prefix.getDepth()).equals(prefix.fragments);
	}

	@Override
	public Iterator<EvaluableExpression> iterator() {
		return this.fragments.iterator();
	}

	@Override
	public void replace(EvaluableExpression toReplace, EvaluableExpression replaceFragment) {
		super.replace(toReplace, replaceFragment);

		PathExpression pathToFind = this.wrapAsPath(toReplace);
		PathExpression replacePath = this.wrapAsPath(replaceFragment);
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
	protected void toString(StringBuilder builder) {
		for (EvaluableExpression fragment : this.fragments)
			fragment.toString(builder);
	}

	private PathExpression wrapAsPath(EvaluableExpression expression) {
		if (expression instanceof PathExpression)
			return (PathExpression) expression;
		return new PathExpression(expression);
	}
}