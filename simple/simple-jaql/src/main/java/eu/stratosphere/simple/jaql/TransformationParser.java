package eu.stratosphere.simple.jaql;

import java.util.List;

import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.CopyField;
import com.ibm.jaql.lang.expr.core.CopyRecord;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.NameValueBinding;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;

import eu.stratosphere.reflect.TypeHandler;
import eu.stratosphere.reflect.TypeSpecificHandler;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.FieldAccess;
import eu.stratosphere.sopremo.expressions.Mapping;
import eu.stratosphere.sopremo.expressions.Path;
import eu.stratosphere.sopremo.expressions.Transformation;
import eu.stratosphere.sopremo.expressions.ValueAssignment;

class TransformationParser implements JaqlToSopremoParser<Transformation> {

	private final class TransformationConverter implements MappingConverter<TransformExpr> {
		// ?
		@Override
		public Transformation convert(TransformExpr expr, List<Mapping> childOperators) {
			return (Transformation) childOperators.get(childOperators.size() - 1);
		}
	}

	private final class AllFieldsCopyConverter implements MappingConverter<CopyRecord> {
		// input2.*
		@Override
		public ValueAssignment convert(CopyRecord expr, List<Mapping> childTransformations) {
			Path path = (Path) TransformationParser.this.queryParser.parsePath(expr.recExpr());
			return new ValueAssignment(ValueAssignment.COPY_ALL_FIELDS, path);
		}
	}

	private final class FieldCopyConverter implements MappingConverter<CopyField> {
		// id
		@Override
		public ValueAssignment convert(CopyField expr, List<Mapping> childTransformations) {
			String fieldName = ((ConstExpr) expr.nameExpr()).value.toString();
			Path path = (Path) TransformationParser.this.queryParser.parsePath(expr.recExpr());
			path.add(new FieldAccess(fieldName));
			return new ValueAssignment(fieldName, path);
		}
	}

	private final class ValueMappingConverter implements MappingConverter<NameValueBinding> {
		// id: calcId(), person: { ... }
		@Override
		public Mapping convert(NameValueBinding expr, List<Mapping> childTransformations) {
			if (!childTransformations.isEmpty()) {
				Mapping transformation = childTransformations.get(0);
				transformation.setTarget(((ConstExpr) expr.nameExpr()).value.toString());
				return transformation;
			}
			return new ValueAssignment(((ConstExpr) expr.nameExpr()).value.toString(),
				TransformationParser.this.queryParser.parsePath(expr.valueExpr()));
		}
	}

	private final class ObjectMappingConverter implements MappingConverter<RecordExpr> {
		// ... into { mapping, ... }
		@Override
		public Transformation convert(RecordExpr expr, List<Mapping> childTransformations) {
			Transformation transformation = new Transformation();
			for (Mapping mapping : childTransformations)
				transformation.addMapping(mapping);
			return transformation;
		}
	}

	private static interface MappingConverter<I extends Expr> extends TypeHandler<I, Mapping> {
		public Mapping convert(I expr, List<Mapping> childTransformations);
	}

	private QueryParser queryParser;

	private TypeSpecificHandler<Expr, Mapping, TypeHandler<Expr, Mapping>> transformationConverter = new TypeSpecificHandler<Expr, Mapping, TypeHandler<Expr, Mapping>>();

	@SuppressWarnings("unchecked")
	public TransformationParser(QueryParser queryParser) {
		this.queryParser = queryParser;
		this.transformationConverter.registerAll(new ObjectMappingConverter(), new ValueMappingConverter(),
			new FieldCopyConverter(), new AllFieldsCopyConverter(), new TransformationConverter());
	}

	@Override
	public Transformation parse(Expr expr) {
		Transformation transformation = (Transformation) this.transformationConverter.handleRecursively(
			ExprNavigator.INSTANCE, expr);
		if (transformation == null) {
			transformation = new Transformation();
			EvaluableExpression path = this.queryParser.parsePath(expr);
			transformation.addMapping(new ValueAssignment(path));
		}
		return transformation;
	}

}
