package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.codehaus.jackson.JsonNode;


public class Path extends EvaluableExpression {

	private List<EvaluableExpression> fragments = new ArrayList<EvaluableExpression>();

	public Path(List<EvaluableExpression> fragments) {
		this.fragments = fragments;
		for (EvaluableExpression evaluableExpression : fragments) {
			if (evaluableExpression instanceof Path)
				throw new IllegalArgumentException();
		}
	}

	public Path(EvaluableExpression... fragments) {
		this.fragments = Arrays.asList(fragments);
		for (EvaluableExpression evaluableExpression : fragments) {
			if (evaluableExpression instanceof Path)
				throw new IllegalArgumentException();
		}
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
	public JsonNode evaluate(JsonNode node) {
		JsonNode fragmentNode = node;
		for (EvaluableExpression fragment : this.fragments)
			fragmentNode = fragment.evaluate(fragmentNode);
		return fragmentNode;
	}

	public EvaluableExpression getSelector(int distance) {
		return this.fragments.get(distance);
	}

	public List<EvaluableExpression> getFragments() {
		return fragments;
	}

	public void add(EvaluableExpression fragment) {
		this.fragments.add(fragment);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + fragments.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Path other = (Path) obj;
		return fragments.equals(other.fragments);
	}

	@Override
	protected void toString(StringBuilder builder) {
		for (EvaluableExpression fragment : this.fragments)
			fragment.toString(builder);
	}
}