package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class BinarySparseMatrix<T> {
	private final Map<T, Set<T>> sparseMatrix = new HashMap<T, Set<T>>();

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final BinarySparseMatrix<?> other = (BinarySparseMatrix<?>) obj;
		if (this.sparseMatrix.size() != other.sparseMatrix.size())
			return false;

		Set<T> rows = this.getRows();
		for (T row : rows)
			if (!this.get(row).equals(this.sparseMatrix.get(row)))
				return false;
		return true;
	}

	public Set<T> get(final T n) {
		return this.sparseMatrix.get(n);
	}

	public Set<T> getRows() {
		return this.sparseMatrix.keySet();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.sparseMatrix.hashCode();
		return result;
	}

	public boolean isSet(final T n1, final T n2) {
		final Set<T> set = this.sparseMatrix.get(n1);
		return set != null && set.contains(n2);
	}

	void makeSymmetric() {
		final Set<T> rows = new HashSet<T>(this.getRows());
		for (final T row : rows)
			for (final T column : this.get(row))
				this.set(column, row);
	}

	public void set(final T n1, final T n2) {
		Set<T> set = this.sparseMatrix.get(n1);
		if (set == null)
			this.sparseMatrix.put(n1, set = new HashSet<T>());
		set.add(n2);
	}

	public void setAll(final T n1, final Set<T> n2) {
		Set<T> set = this.sparseMatrix.get(n1);
		if (set == null)
			this.sparseMatrix.put(n1, set = new HashSet<T>());
		set.addAll(n2);
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder("[\n");
		for (final T row : this.getRows())
			builder.append("[").append(row).append(": ").append(this.get(row)).append("]\n");
		return builder.append("]").toString();
	}
}