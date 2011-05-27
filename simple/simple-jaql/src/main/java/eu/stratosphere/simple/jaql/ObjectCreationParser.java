package eu.stratosphere.simple.jaql;

import java.util.List;

import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.CopyField;
import com.ibm.jaql.lang.expr.core.CopyRecord;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.NameValueBinding;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;

import eu.stratosphere.dag.converter.NodeConverter;
import eu.stratosphere.dag.converter.GraphConverter;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.expressions.FieldAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.Path;

class ObjectCreationParser implements JaqlToSopremoParser<EvaluableExpression> {

	private final class ObjectCreationConverter implements MappingConverter<TransformExpr> {
		// ?
		@Override
		public ObjectCreation convertNode(TransformExpr expr, List<Object> childOperators) {
			return (ObjectCreation) childOperators.get(childOperators.size() - 1);
		}
	}

	private final class AllFieldsCopyConverter implements MappingConverter<CopyRecord> {
		// input2.*
		@Override
		public ObjectCreation.Mapping convertNode(CopyRecord expr, List<Object> childEvaluableExpressions) {
			Path path = (Path) ObjectCreationParser.this.queryParser.parsePath(expr.recExpr());
			return new ObjectCreation.CopyFields(path);
		}
	}

	private final class FieldCopyConverter implements MappingConverter<CopyField> {
		// id
		@Override
		public ObjectCreation.Mapping convertNode(CopyField expr, List<Object> childEvaluableExpressions) {
			String fieldName = ((ConstExpr) expr.nameExpr()).value.toString();
			Path path = (Path) ObjectCreationParser.this.queryParser.parsePath(expr.recExpr());
			path.add(new FieldAccess(fieldName));
			return new ObjectCreation.Mapping(fieldName, path);
		}
	}

	private final class ValueMappingConverter implements MappingConverter<NameValueBinding> {
		// id: calcId(), person: { ... }
		@Override
		public ObjectCreation.Mapping convertNode(NameValueBinding expr, List<Object> childEvaluableExpressions) {
			String fieldName = ((ConstExpr) expr.nameExpr()).value.toString();
			if (!childEvaluableExpressions.isEmpty())
				return new ObjectCreation.Mapping(fieldName, (EvaluableExpression) childEvaluableExpressions.get(0));
			EvaluableExpression expression = ObjectCreationParser.this.queryParser.parsePath(expr.valueExpr());
			return new ObjectCreation.Mapping(fieldName, expression);
		}
	}

	private final class ObjectMappingConverter implements MappingConverter<RecordExpr> {
		// ... into { mapping, ... }
		@Override
		public ObjectCreation convertNode(RecordExpr expr, List<Object> childEvaluableExpressions) {
			ObjectCreation creation = new ObjectCreation();
			for (Object mapping : childEvaluableExpressions)
				if (mapping instanceof ObjectCreation.Mapping)
					creation.addMapping((Mapping) mapping);
			return creation;
		}
	}

	private static interface MappingConverter<I extends Expr> extends NodeConverter<I, Object> {
//		public Object convertNode(I expr, List<Object> childEvaluableExpressions);
	}

	private QueryParser queryParser;

	private GraphConverter<Expr, Object> objectCreationConverter = new GraphConverter<Expr, Object>();

	@SuppressWarnings("unchecked")
	public ObjectCreationParser(QueryParser queryParser) {
		this.queryParser = queryParser;
		this.objectCreationConverter.registerAll(new ObjectMappingConverter(), new ValueMappingConverter(),
			new FieldCopyConverter(), new AllFieldsCopyConverter(), new ObjectCreationConverter());
	}

	@Override
	public EvaluableExpression parse(Expr expr) {
		EvaluableExpression evaluableExpression = (EvaluableExpression) this.objectCreationConverter.convertGraph(
			expr, ExprNavigator.INSTANCE);
		if (evaluableExpression == null)
			return this.queryParser.parsePath(expr);
		return evaluableExpression;
	}

}
