package eu.stratosphere.sopremo;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.dag.Navigator;
import eu.stratosphere.reflect.TypeHandler;
import eu.stratosphere.reflect.TypeSpecificHandler;

public class Transformation extends Mapping {
	public static final Transformation IDENTITY = new Transformation();

	private List<Mapping> mappings = new ArrayList<Mapping>();

	private boolean array;

	public Transformation() {
		this(NO_TARGET);
	}

	public Transformation(String target) {
		super(target);
	}

	public JsonPath asPath() {
		if (getMappingSize() == 0)
			return null;

		Mapping mapping = getMapping(0);
		if (mapping instanceof ValueAssignment)
			return ((ValueAssignment) mapping).getTransformation();
		return null;
	}

	// public Transformation(boolean array) {
	// }

	public void addMapping(Mapping mapping) {
		this.mappings.add(mapping);
	}

	public List<Mapping> getMappings() {
		return mappings;
	}

	public int getMappingSize() {
		return this.mappings.size();
	}

	public Mapping getMapping(int index) {
		return this.mappings.get(index);
	}

	public Object simplify() {
		if (getTarget() == null && mappings.size() == 1 && mappings.get(0).getTarget() == null
			&& mappings.get(0) instanceof ValueAssignment)
			return ((ValueAssignment) mappings.get(0)).getTransformation();
		return this;
	}

	@Override
	public String toString() {
		if (this.getTarget() == NO_TARGET)
			return this.mappings.toString();
		return String.format("%s=%s", this.getTarget(), this.mappings);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (array ? 1231 : 1237);
		result = prime * result + mappings.hashCode();
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
		Transformation other = (Transformation) obj;
		return super.equals(obj) && array == other.array && mappings.equals(other.mappings);
	}

	private static TypeSpecificHandler<Mapping, Mapping, TypeHandler<Mapping, Mapping>> PathReplacer = new TypeSpecificHandler<Mapping, Mapping, TypeHandler<Mapping, Mapping>>();

	static {
		PathReplacer.register(new TypeHandler<ValueAssignment, Mapping>() {
			public ValueAssignment replace(ValueAssignment assignment, List<Mapping> mapping, JsonPath toReplace,
					JsonPath replaceFragment) {
				assignment.setTransformation(JsonPath.replace(assignment.getTransformation(), toReplace,
					replaceFragment));
				return assignment;
			}
		}, ValueAssignment.class);
	}

	public void replace(JsonPath toReplace, JsonPath replaceFragment) {
		PathReplacer.handleRecursively(new MappingNavigator(), this, toReplace, replaceFragment);
	}

	private static final class MappingNavigator implements Navigator<Mapping> {
		private static final Iterable<Mapping> EMPTY = new ArrayList<Mapping>();

		@Override
		public Iterable<Mapping> getConnectedNodes(Mapping node) {
			if (node instanceof Transformation)
				return ((Transformation) node).getMappings();
			return EMPTY;
		}
	}
}
