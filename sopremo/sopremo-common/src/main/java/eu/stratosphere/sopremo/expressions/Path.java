package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.codehaus.jackson.JsonNode;

import eu.stratosphere.sopremo.Evaluable;
import eu.stratosphere.sopremo.EvaluationContext;

public class Path extends ContainerExpression {

	private List<EvaluableExpression> fragments = new ArrayList<EvaluableExpression>();

	public Path(List<EvaluableExpression> fragments) {
		this.fragments = fragments;
		for (Evaluable evaluableExpression : fragments)
			if (evaluableExpression instanceof Path)
				throw new IllegalArgumentException();
	}

	public Path(EvaluableExpression... fragments) {
		this.fragments = Arrays.asList(fragments);
		for (Evaluable evaluableExpression : fragments)
			if (evaluableExpression instanceof Path)
				throw new IllegalArgumentException();
	}

	@Override
	public void replace(EvaluableExpression toReplace, EvaluableExpression replaceFragment) {
		this.fragments = replace(this, wrapAsPath(toReplace), wrapAsPath(replaceFragment)).fragments;
	}

	private Path wrapAsPath(EvaluableExpression expression) {
		if (expression instanceof Path)
			return (Path) expression;
		return new Path(expression);
	}

	public static Path replace(Path path, Path pathToFind, Path replacePath) {
		List<EvaluableExpression> fragments = null;

		final int size = path.fragments.size() - pathToFind.fragments.size() + 1;
		final int findSize = pathToFind.fragments.size();
		findStartIndex: for (int startIndex = 0; startIndex < size; startIndex++) {
			for (int index = 0; index < findSize; index++)
				if (!path.fragments.get(startIndex + index).equals(pathToFind.fragments.get(index)))
					continue findStartIndex;

			if (fragments == null)
				fragments = new ArrayList<EvaluableExpression>(path.fragments);
			fragments.subList(startIndex, startIndex + findSize).clear();
			fragments.addAll(startIndex, replacePath.fragments);
			startIndex += replacePath.fragments.size();
		}

		// no replacements done
		if (fragments == null)
			return path;
		return new Path(fragments);
	}

	public boolean isPrefix(Path prefix) {
		if (this.fragments.size() < prefix.getDepth())
			return false;
		return this.fragments.subList(0, prefix.getDepth()).equals(prefix.fragments);
	}

	public int getDepth() {
		return this.fragments.size();
	}

	@Override
	public JsonNode evaluate(JsonNode node, EvaluationContext context) {
		JsonNode fragmentNode = node;
		for (Evaluable fragment : this.fragments)
			fragmentNode = fragment.evaluate(fragmentNode, context);
		return fragmentNode;
	}

	public EvaluableExpression getFragment(int index) {
		return this.fragments.get(index);
	}

	public List<EvaluableExpression> getFragments() {
		return this.fragments;
	}

	public void add(EvaluableExpression fragment) {
		this.fragments.add(fragment);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.fragments.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		Path other = (Path) obj;
		return this.fragments.equals(other.fragments);
	}

	@Override
	protected void toString(StringBuilder builder) {
		for (EvaluableExpression fragment : this.fragments)
			fragment.toString(builder);
	}
}