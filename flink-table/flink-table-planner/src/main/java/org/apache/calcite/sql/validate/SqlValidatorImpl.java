/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.validate;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Feature;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ModifiableViewTable;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAccessEnum;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSampleSpec;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.AssignableOperandTypeChecker;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.sql.validate.implicit.TypeCoercions;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.BitString;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.slf4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.apache.calcite.sql.SqlUtil.stripAs;
import static org.apache.calcite.util.Static.RESOURCE;

/*
 * THIS FILE HAS BEEN COPIED FROM THE APACHE CALCITE PROJECT UNTIL CALCITE-2707 IS FIXED.
 * (Added lines: 6016-6022)
 */

/**
 * Default implementation of {@link SqlValidator}.
 */
public class SqlValidatorImpl implements SqlValidatorWithHints {
	//~ Static fields/initializers ---------------------------------------------

	public static final Logger TRACER = CalciteTrace.PARSER_LOGGER;

	/**
	 * Alias generated for the source table when rewriting UPDATE to MERGE.
	 */
	public static final String UPDATE_SRC_ALIAS = "SYS$SRC";

	/**
	 * Alias generated for the target table when rewriting UPDATE to MERGE if no
	 * alias was specified by the user.
	 */
	public static final String UPDATE_TGT_ALIAS = "SYS$TGT";

	/**
	 * Alias prefix generated for source columns when rewriting UPDATE to MERGE.
	 */
	public static final String UPDATE_ANON_PREFIX = "SYS$ANON";

	//~ Instance fields --------------------------------------------------------

	private final SqlOperatorTable opTab;
	final SqlValidatorCatalogReader catalogReader;

	/**
	 * Maps ParsePosition strings to the {@link SqlIdentifier} identifier
	 * objects at these positions
	 */
	protected final Map<String, IdInfo> idPositions = new HashMap<>();

	/**
	 * Maps {@link SqlNode query node} objects to the {@link SqlValidatorScope}
	 * scope created from them.
	 */
	protected final Map<SqlNode, SqlValidatorScope> scopes =
		new IdentityHashMap<>();

	/**
	 * Maps a {@link SqlSelect} node to the scope used by its WHERE and HAVING
	 * clauses.
	 */
	private final Map<SqlSelect, SqlValidatorScope> whereScopes =
		new IdentityHashMap<>();

	/**
	 * Maps a {@link SqlSelect} node to the scope used by its GROUP BY clause.
	 */
	private final Map<SqlSelect, SqlValidatorScope> groupByScopes =
		new IdentityHashMap<>();

	/**
	 * Maps a {@link SqlSelect} node to the scope used by its SELECT and HAVING
	 * clauses.
	 */
	private final Map<SqlSelect, SqlValidatorScope> selectScopes =
		new IdentityHashMap<>();

	/**
	 * Maps a {@link SqlSelect} node to the scope used by its ORDER BY clause.
	 */
	private final Map<SqlSelect, SqlValidatorScope> orderScopes =
		new IdentityHashMap<>();

	/**
	 * Maps a {@link SqlSelect} node that is the argument to a CURSOR
	 * constructor to the scope of the result of that select node
	 */
	private final Map<SqlSelect, SqlValidatorScope> cursorScopes =
		new IdentityHashMap<>();

	/**
	 * The name-resolution scope of a LATERAL TABLE clause.
	 */
	private TableScope tableScope = null;

	/**
	 * Maps a {@link SqlNode node} to the
	 * {@link SqlValidatorNamespace namespace} which describes what columns they
	 * contain.
	 */
	protected final Map<SqlNode, SqlValidatorNamespace> namespaces =
		new IdentityHashMap<>();

	/**
	 * Set of select expressions used as cursor definitions. In standard SQL,
	 * only the top-level SELECT is a cursor; Calcite extends this with
	 * cursors as inputs to table functions.
	 */
	private final Set<SqlNode> cursorSet = Sets.newIdentityHashSet();

	/**
	 * Stack of objects that maintain information about function calls. A stack
	 * is needed to handle nested function calls. The function call currently
	 * being validated is at the top of the stack.
	 */
	protected final Deque<FunctionParamInfo> functionCallStack =
		new ArrayDeque<>();

	private int nextGeneratedId;
	protected final RelDataTypeFactory typeFactory;

	/** The type of dynamic parameters until a type is imposed on them. */
	protected final RelDataType unknownType;
	private final RelDataType booleanType;

	/**
	 * Map of derived RelDataType for each node. This is an IdentityHashMap
	 * since in some cases (such as null literals) we need to discriminate by
	 * instance.
	 */
	private final Map<SqlNode, RelDataType> nodeToTypeMap =
		new IdentityHashMap<>();
	private final AggFinder aggFinder;
	private final AggFinder aggOrOverFinder;
	private final AggFinder aggOrOverOrGroupFinder;
	private final AggFinder groupFinder;
	private final AggFinder overFinder;
	private final SqlConformance conformance;
	private final Map<SqlNode, SqlNode> originalExprs = new HashMap<>();

	private SqlNode top;

	// REVIEW jvs 30-June-2006: subclasses may override shouldExpandIdentifiers
	// in a way that ignores this; we should probably get rid of the protected
	// method and always use this variable (or better, move preferences like
	// this to a separate "parameter" class)
	protected boolean expandIdentifiers;

	protected boolean expandColumnReferences;

	private boolean rewriteCalls;

	private NullCollation nullCollation = NullCollation.HIGH;

	// TODO jvs 11-Dec-2008:  make this local to performUnconditionalRewrites
	// if it's OK to expand the signature of that method.
	private boolean validatingSqlMerge;

	private boolean inWindow;                        // Allow nested aggregates

	private final SqlValidatorImpl.ValidationErrorFunction validationErrorFunction =
		new SqlValidatorImpl.ValidationErrorFunction();

	// TypeCoercion instance used for implicit type coercion.
	private TypeCoercion typeCoercion;

	// Flag saying if we enable the implicit type coercion.
	private boolean enableTypeCoercion;

	//~ Constructors -----------------------------------------------------------

	/**
	 * Creates a validator.
	 *
	 * @param opTab         Operator table
	 * @param catalogReader Catalog reader
	 * @param typeFactory   Type factory
	 * @param conformance   Compatibility mode
	 */
	protected SqlValidatorImpl(
		SqlOperatorTable opTab,
		SqlValidatorCatalogReader catalogReader,
		RelDataTypeFactory typeFactory,
		SqlConformance conformance) {
		this.opTab = Objects.requireNonNull(opTab);
		this.catalogReader = Objects.requireNonNull(catalogReader);
		this.typeFactory = Objects.requireNonNull(typeFactory);
		this.conformance = Objects.requireNonNull(conformance);

		unknownType = typeFactory.createUnknownType();
		booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

		rewriteCalls = true;
		expandColumnReferences = true;
		final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
		aggFinder = new AggFinder(opTab, false, true, false, null, nameMatcher);
		aggOrOverFinder = new AggFinder(opTab, true, true, false, null, nameMatcher);
		overFinder = new AggFinder(opTab, true, false, false, aggOrOverFinder, nameMatcher);
		groupFinder = new AggFinder(opTab, false, false, true, null, nameMatcher);
		aggOrOverOrGroupFinder = new AggFinder(opTab, true, true, true, null, nameMatcher);
		this.enableTypeCoercion = catalogReader.getConfig() == null
			|| catalogReader.getConfig().typeCoercion();
		this.typeCoercion = TypeCoercions.getTypeCoercion(this, conformance);
	}

	//~ Methods ----------------------------------------------------------------

	public SqlConformance getConformance() {
		return conformance;
	}

	public SqlValidatorCatalogReader getCatalogReader() {
		return catalogReader;
	}

	public SqlOperatorTable getOperatorTable() {
		return opTab;
	}

	public RelDataTypeFactory getTypeFactory() {
		return typeFactory;
	}

	public RelDataType getUnknownType() {
		return unknownType;
	}

	public SqlNodeList expandStar(
		SqlNodeList selectList,
		SqlSelect select,
		boolean includeSystemVars) {
		final List<SqlNode> list = new ArrayList<>();
		final List<Map.Entry<String, RelDataType>> types = new ArrayList<>();
		for (int i = 0; i < selectList.size(); i++) {
			final SqlNode selectItem = selectList.get(i);
			final RelDataType originalType = getValidatedNodeTypeIfKnown(selectItem);
			expandSelectItem(
				selectItem,
				select,
				Util.first(originalType, unknownType),
				list,
				catalogReader.nameMatcher().createSet(),
				types,
				includeSystemVars);
		}
		getRawSelectScope(select).setExpandedSelectList(list);
		return new SqlNodeList(list, SqlParserPos.ZERO);
	}

	// implement SqlValidator
	public void declareCursor(SqlSelect select, SqlValidatorScope parentScope) {
		cursorSet.add(select);

		// add the cursor to a map that maps the cursor to its select based on
		// the position of the cursor relative to other cursors in that call
		FunctionParamInfo funcParamInfo = functionCallStack.peek();
		Map<Integer, SqlSelect> cursorMap = funcParamInfo.cursorPosToSelectMap;
		int numCursors = cursorMap.size();
		cursorMap.put(numCursors, select);

		// create a namespace associated with the result of the select
		// that is the argument to the cursor constructor; register it
		// with a scope corresponding to the cursor
		SelectScope cursorScope = new SelectScope(parentScope, null, select);
		cursorScopes.put(select, cursorScope);
		final SelectNamespace selectNs = createSelectNamespace(select, select);
		String alias = deriveAlias(select, nextGeneratedId++);
		registerNamespace(cursorScope, alias, selectNs, false);
	}

	// implement SqlValidator
	public void pushFunctionCall() {
		FunctionParamInfo funcInfo = new FunctionParamInfo();
		functionCallStack.push(funcInfo);
	}

	// implement SqlValidator
	public void popFunctionCall() {
		functionCallStack.pop();
	}

	// implement SqlValidator
	public String getParentCursor(String columnListParamName) {
		FunctionParamInfo funcParamInfo = functionCallStack.peek();
		Map<String, String> parentCursorMap =
			funcParamInfo.columnListParamToParentCursorMap;
		return parentCursorMap.get(columnListParamName);
	}

	/**
	 * If <code>selectItem</code> is "*" or "TABLE.*", expands it and returns
	 * true; otherwise writes the unexpanded item.
	 *
	 * @param selectItem        Select-list item
	 * @param select            Containing select clause
	 * @param selectItems       List that expanded items are written to
	 * @param aliases           Set of aliases
	 * @param fields            List of field names and types, in alias order
	 * @param includeSystemVars If true include system vars in lists
	 * @return Whether the node was expanded
	 */
	private boolean expandSelectItem(
		final SqlNode selectItem,
		SqlSelect select,
		RelDataType targetType,
		List<SqlNode> selectItems,
		Set<String> aliases,
		List<Map.Entry<String, RelDataType>> fields,
		final boolean includeSystemVars) {
		final SelectScope scope = (SelectScope) getWhereScope(select);
		if (expandStar(selectItems, aliases, fields, includeSystemVars, scope,
			selectItem)) {
			return true;
		}

		// Expand the select item: fully-qualify columns, and convert
		// parentheses-free functions such as LOCALTIME into explicit function
		// calls.
		SqlNode expanded = expand(selectItem, scope);
		final String alias =
			deriveAlias(
				selectItem,
				aliases.size());

		// If expansion has altered the natural alias, supply an explicit 'AS'.
		final SqlValidatorScope selectScope = getSelectScope(select);
		if (expanded != selectItem) {
			String newAlias =
				deriveAlias(
					expanded,
					aliases.size());
			if (!newAlias.equals(alias)) {
				expanded =
					SqlStdOperatorTable.AS.createCall(
						selectItem.getParserPosition(),
						expanded,
						new SqlIdentifier(alias, SqlParserPos.ZERO));
				deriveTypeImpl(selectScope, expanded);
			}
		}

		selectItems.add(expanded);
		aliases.add(alias);

		if (expanded != null) {
			inferUnknownTypes(targetType, scope, expanded);
		}
		final RelDataType type = deriveType(selectScope, expanded);
		setValidatedNodeType(expanded, type);
		fields.add(Pair.of(alias, type));
		return false;
	}

	private boolean expandStar(List<SqlNode> selectItems, Set<String> aliases,
		List<Map.Entry<String, RelDataType>> fields, boolean includeSystemVars,
		SelectScope scope, SqlNode node) {
		if (!(node instanceof SqlIdentifier)) {
			return false;
		}
		final SqlIdentifier identifier = (SqlIdentifier) node;
		if (!identifier.isStar()) {
			return false;
		}
		final SqlParserPos startPosition = identifier.getParserPosition();
		switch (identifier.names.size()) {
			case 1:
				boolean hasDynamicStruct = false;
				for (ScopeChild child : scope.children) {
					final int before = fields.size();
					if (child.namespace.getRowType().isDynamicStruct()) {
						hasDynamicStruct = true;
						// don't expand star if the underneath table is dynamic.
						// Treat this star as a special field in validation/conversion and
						// wait until execution time to expand this star.
						final SqlNode exp =
							new SqlIdentifier(
								ImmutableList.of(child.name,
									DynamicRecordType.DYNAMIC_STAR_PREFIX),
								startPosition);
						addToSelectList(
							selectItems,
							aliases,
							fields,
							exp,
							scope,
							includeSystemVars);
					} else {
						final SqlNode from = child.namespace.getNode();
						final SqlValidatorNamespace fromNs = getNamespace(from, scope);
						assert fromNs != null;
						final RelDataType rowType = fromNs.getRowType();
						for (RelDataTypeField field : rowType.getFieldList()) {
							String columnName = field.getName();

							// TODO: do real implicit collation here
							final SqlIdentifier exp =
								new SqlIdentifier(
									ImmutableList.of(child.name, columnName),
									startPosition);
							// Don't add expanded rolled up columns
							if (!isRolledUpColumn(exp, scope)) {
								addOrExpandField(
									selectItems,
									aliases,
									fields,
									includeSystemVars,
									scope,
									exp,
									field);
							}
						}
					}
					if (child.nullable) {
						for (int i = before; i < fields.size(); i++) {
							final Map.Entry<String, RelDataType> entry = fields.get(i);
							final RelDataType type = entry.getValue();
							if (!type.isNullable()) {
								fields.set(i,
									Pair.of(entry.getKey(),
										typeFactory.createTypeWithNullability(type, true)));
							}
						}
					}
				}
				// If NATURAL JOIN or USING is present, move key fields to the front of
				// the list, per standard SQL. Disabled if there are dynamic fields.
				if (!hasDynamicStruct || Bug.CALCITE_2400_FIXED) {
					new Permute(scope.getNode().getFrom(), 0).permute(selectItems, fields);
				}
				return true;

			default:
				final SqlIdentifier prefixId = identifier.skipLast(1);
				final SqlValidatorScope.ResolvedImpl resolved =
					new SqlValidatorScope.ResolvedImpl();
				final SqlNameMatcher nameMatcher =
					scope.validator.catalogReader.nameMatcher();
				scope.resolve(prefixId.names, nameMatcher, true, resolved);
				if (resolved.count() == 0) {
					// e.g. "select s.t.* from e"
					// or "select r.* from e"
					throw newValidationError(prefixId,
						RESOURCE.unknownIdentifier(prefixId.toString()));
				}
				final RelDataType rowType = resolved.only().rowType();
				if (rowType.isDynamicStruct()) {
					// don't expand star if the underneath table is dynamic.
					addToSelectList(
						selectItems,
						aliases,
						fields,
						prefixId.plus(DynamicRecordType.DYNAMIC_STAR_PREFIX, startPosition),
						scope,
						includeSystemVars);
				} else if (rowType.isStruct()) {
					for (RelDataTypeField field : rowType.getFieldList()) {
						String columnName = field.getName();

						// TODO: do real implicit collation here
						addOrExpandField(
							selectItems,
							aliases,
							fields,
							includeSystemVars,
							scope,
							prefixId.plus(columnName, startPosition),
							field);
					}
				} else {
					throw newValidationError(prefixId, RESOURCE.starRequiresRecordType());
				}
				return true;
		}
	}

	private SqlNode maybeCast(SqlNode node, RelDataType currentType,
		RelDataType desiredType) {
		return currentType.equals(desiredType)
			|| (currentType.isNullable() != desiredType.isNullable()
				    && typeFactory.createTypeWithNullability(currentType,
			desiredType.isNullable()).equals(desiredType))
			? node
			: SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO,
			node, SqlTypeUtil.convertTypeToSpec(desiredType));
	}

	private boolean addOrExpandField(List<SqlNode> selectItems, Set<String> aliases,
		List<Map.Entry<String, RelDataType>> fields, boolean includeSystemVars,
		SelectScope scope, SqlIdentifier id, RelDataTypeField field) {
		switch (field.getType().getStructKind()) {
			case PEEK_FIELDS:
			case PEEK_FIELDS_DEFAULT:
				final SqlNode starExp = id.plusStar();
				expandStar(
					selectItems,
					aliases,
					fields,
					includeSystemVars,
					scope,
					starExp);
				return true;

			default:
				addToSelectList(
					selectItems,
					aliases,
					fields,
					id,
					scope,
					includeSystemVars);
		}

		return false;
	}

	public SqlNode validate(SqlNode topNode) {
		SqlValidatorScope scope = new EmptyScope(this);
		scope = new CatalogScope(scope, ImmutableList.of("CATALOG"));
		final SqlNode topNode2 = validateScopedExpression(topNode, scope);
		final RelDataType type = getValidatedNodeType(topNode2);
		Util.discard(type);
		return topNode2;
	}

	public List<SqlMoniker> lookupHints(SqlNode topNode, SqlParserPos pos) {
		SqlValidatorScope scope = new EmptyScope(this);
		SqlNode outermostNode = performUnconditionalRewrites(topNode, false);
		cursorSet.add(outermostNode);
		if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
			registerQuery(
				scope,
				null,
				outermostNode,
				outermostNode,
				null,
				false);
		}
		final SqlValidatorNamespace ns = getNamespace(outermostNode);
		if (ns == null) {
			throw new AssertionError("Not a query: " + outermostNode);
		}
		Collection<SqlMoniker> hintList = Sets.newTreeSet(SqlMoniker.COMPARATOR);
		lookupSelectHints(ns, pos, hintList);
		return ImmutableList.copyOf(hintList);
	}

	public SqlMoniker lookupQualifiedName(SqlNode topNode, SqlParserPos pos) {
		final String posString = pos.toString();
		IdInfo info = idPositions.get(posString);
		if (info != null) {
			final SqlQualified qualified = info.scope.fullyQualify(info.id);
			return new SqlIdentifierMoniker(qualified.identifier);
		} else {
			return null;
		}
	}

	/**
	 * Looks up completion hints for a syntactically correct select SQL that has
	 * been parsed into an expression tree.
	 *
	 * @param select   the Select node of the parsed expression tree
	 * @param pos      indicates the position in the sql statement we want to get
	 *                 completion hints for
	 * @param hintList list of {@link SqlMoniker} (sql identifiers) that can
	 *                 fill in at the indicated position
	 */
	void lookupSelectHints(
		SqlSelect select,
		SqlParserPos pos,
		Collection<SqlMoniker> hintList) {
		IdInfo info = idPositions.get(pos.toString());
		if ((info == null) || (info.scope == null)) {
			SqlNode fromNode = select.getFrom();
			final SqlValidatorScope fromScope = getFromScope(select);
			lookupFromHints(fromNode, fromScope, pos, hintList);
		} else {
			lookupNameCompletionHints(info.scope, info.id.names,
				info.id.getParserPosition(), hintList);
		}
	}

	private void lookupSelectHints(
		SqlValidatorNamespace ns,
		SqlParserPos pos,
		Collection<SqlMoniker> hintList) {
		final SqlNode node = ns.getNode();
		if (node instanceof SqlSelect) {
			lookupSelectHints((SqlSelect) node, pos, hintList);
		}
	}

	private void lookupFromHints(
		SqlNode node,
		SqlValidatorScope scope,
		SqlParserPos pos,
		Collection<SqlMoniker> hintList) {
		if (node == null) {
			// This can happen in cases like "select * _suggest_", so from clause is absent
			return;
		}
		final SqlValidatorNamespace ns = getNamespace(node);
		if (ns.isWrapperFor(IdentifierNamespace.class)) {
			IdentifierNamespace idNs = ns.unwrap(IdentifierNamespace.class);
			final SqlIdentifier id = idNs.getId();
			for (int i = 0; i < id.names.size(); i++) {
				if (pos.toString().equals(
					id.getComponent(i).getParserPosition().toString())) {
					final List<SqlMoniker> objNames = new ArrayList<>();
					SqlValidatorUtil.getSchemaObjectMonikers(
						getCatalogReader(),
						id.names.subList(0, i + 1),
						objNames);
					for (SqlMoniker objName : objNames) {
						if (objName.getType() != SqlMonikerType.FUNCTION) {
							hintList.add(objName);
						}
					}
					return;
				}
			}
		}
		switch (node.getKind()) {
			case JOIN:
				lookupJoinHints((SqlJoin) node, scope, pos, hintList);
				break;
			default:
				lookupSelectHints(ns, pos, hintList);
				break;
		}
	}

	private void lookupJoinHints(
		SqlJoin join,
		SqlValidatorScope scope,
		SqlParserPos pos,
		Collection<SqlMoniker> hintList) {
		SqlNode left = join.getLeft();
		SqlNode right = join.getRight();
		SqlNode condition = join.getCondition();
		lookupFromHints(left, scope, pos, hintList);
		if (hintList.size() > 0) {
			return;
		}
		lookupFromHints(right, scope, pos, hintList);
		if (hintList.size() > 0) {
			return;
		}
		final JoinConditionType conditionType = join.getConditionType();
		final SqlValidatorScope joinScope = scopes.get(join);
		switch (conditionType) {
			case ON:
				condition.findValidOptions(this, joinScope, pos, hintList);
				return;
			default:

				// No suggestions.
				// Not supporting hints for other types such as 'Using' yet.
				return;
		}
	}

	/**
	 * Populates a list of all the valid alternatives for an identifier.
	 *
	 * @param scope    Validation scope
	 * @param names    Components of the identifier
	 * @param pos      position
	 * @param hintList a list of valid options
	 */
	public final void lookupNameCompletionHints(
		SqlValidatorScope scope,
		List<String> names,
		SqlParserPos pos,
		Collection<SqlMoniker> hintList) {
		// Remove the last part of name - it is a dummy
		List<String> subNames = Util.skipLast(names);

		if (subNames.size() > 0) {
			// If there's a prefix, resolve it to a namespace.
			SqlValidatorNamespace ns = null;
			for (String name : subNames) {
				if (ns == null) {
					final SqlValidatorScope.ResolvedImpl resolved =
						new SqlValidatorScope.ResolvedImpl();
					final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
					scope.resolve(ImmutableList.of(name), nameMatcher, false, resolved);
					if (resolved.count() == 1) {
						ns = resolved.only().namespace;
					}
				} else {
					ns = ns.lookupChild(name);
				}
				if (ns == null) {
					break;
				}
			}
			if (ns != null) {
				RelDataType rowType = ns.getRowType();
				if (rowType.isStruct()) {
					for (RelDataTypeField field : rowType.getFieldList()) {
						hintList.add(
							new SqlMonikerImpl(
								field.getName(),
								SqlMonikerType.COLUMN));
					}
				}
			}

			// builtin function names are valid completion hints when the
			// identifier has only 1 name part
			findAllValidFunctionNames(names, this, hintList, pos);
		} else {
			// No prefix; use the children of the current scope (that is,
			// the aliases in the FROM clause)
			scope.findAliases(hintList);

			// If there's only one alias, add all child columns
			SelectScope selectScope =
				SqlValidatorUtil.getEnclosingSelectScope(scope);
			if ((selectScope != null)
				&& (selectScope.getChildren().size() == 1)) {
				RelDataType rowType =
					selectScope.getChildren().get(0).getRowType();
				for (RelDataTypeField field : rowType.getFieldList()) {
					hintList.add(
						new SqlMonikerImpl(
							field.getName(),
							SqlMonikerType.COLUMN));
				}
			}
		}

		findAllValidUdfNames(names, this, hintList);
	}

	private static void findAllValidUdfNames(
		List<String> names,
		SqlValidator validator,
		Collection<SqlMoniker> result) {
		final List<SqlMoniker> objNames = new ArrayList<>();
		SqlValidatorUtil.getSchemaObjectMonikers(
			validator.getCatalogReader(),
			names,
			objNames);
		for (SqlMoniker objName : objNames) {
			if (objName.getType() == SqlMonikerType.FUNCTION) {
				result.add(objName);
			}
		}
	}

	private static void findAllValidFunctionNames(
		List<String> names,
		SqlValidator validator,
		Collection<SqlMoniker> result,
		SqlParserPos pos) {
		// a function name can only be 1 part
		if (names.size() > 1) {
			return;
		}
		for (SqlOperator op : validator.getOperatorTable().getOperatorList()) {
			SqlIdentifier curOpId =
				new SqlIdentifier(
					op.getName(),
					pos);

			final SqlCall call = validator.makeNullaryCall(curOpId);
			if (call != null) {
				result.add(
					new SqlMonikerImpl(
						op.getName(),
						SqlMonikerType.FUNCTION));
			} else {
				if ((op.getSyntax() == SqlSyntax.FUNCTION)
					|| (op.getSyntax() == SqlSyntax.PREFIX)) {
					if (op.getOperandTypeChecker() != null) {
						String sig = op.getAllowedSignatures();
						sig = sig.replaceAll("'", "");
						result.add(
							new SqlMonikerImpl(
								sig,
								SqlMonikerType.FUNCTION));
						continue;
					}
					result.add(
						new SqlMonikerImpl(
							op.getName(),
							SqlMonikerType.FUNCTION));
				}
			}
		}
	}

	public SqlNode validateParameterizedExpression(
		SqlNode topNode,
		final Map<String, RelDataType> nameToTypeMap) {
		SqlValidatorScope scope = new ParameterScope(this, nameToTypeMap);
		return validateScopedExpression(topNode, scope);
	}

	private SqlNode validateScopedExpression(
		SqlNode topNode,
		SqlValidatorScope scope) {
		SqlNode outermostNode = performUnconditionalRewrites(topNode, false);
		cursorSet.add(outermostNode);
		top = outermostNode;
		TRACER.trace("After unconditional rewrite: {}", outermostNode);
		if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
			registerQuery(scope, null, outermostNode, outermostNode, null, false);
		}
		outermostNode.validate(this, scope);
		if (!outermostNode.isA(SqlKind.TOP_LEVEL)) {
			// force type derivation so that we can provide it to the
			// caller later without needing the scope
			deriveType(scope, outermostNode);
		}
		TRACER.trace("After validation: {}", outermostNode);
		return outermostNode;
	}

	public void validateQuery(SqlNode node, SqlValidatorScope scope,
		RelDataType targetRowType) {
		final SqlValidatorNamespace ns = getNamespace(node, scope);
		if (node.getKind() == SqlKind.TABLESAMPLE) {
			List<SqlNode> operands = ((SqlCall) node).getOperandList();
			SqlSampleSpec sampleSpec = SqlLiteral.sampleValue(operands.get(1));
			if (sampleSpec instanceof SqlSampleSpec.SqlTableSampleSpec) {
				validateFeature(RESOURCE.sQLFeature_T613(), node.getParserPosition());
			} else if (sampleSpec
				instanceof SqlSampleSpec.SqlSubstitutionSampleSpec) {
				validateFeature(RESOURCE.sQLFeatureExt_T613_Substitution(),
					node.getParserPosition());
			}
		}

		validateNamespace(ns, targetRowType);
		switch (node.getKind()) {
			case EXTEND:
				// Until we have a dedicated namespace for EXTEND
				deriveType(scope, node);
		}
		if (node == top) {
			validateModality(node);
		}
		validateAccess(
			node,
			ns.getTable(),
			SqlAccessEnum.SELECT);

		if (node.getKind() == SqlKind.SNAPSHOT) {
			SqlSnapshot snapshot = (SqlSnapshot) node;
			SqlNode period = snapshot.getPeriod();
			RelDataType dataType = deriveType(scope, period);
			if (dataType.getSqlTypeName() != SqlTypeName.TIMESTAMP) {
				throw newValidationError(period,
						Static.RESOURCE.illegalExpressionForTemporal(dataType.getSqlTypeName().getName()));
			}
			if (!ns.getTable().isTemporal()) {
				List<String> qualifiedName = ns.getTable().getQualifiedName();
				String tableName = qualifiedName.get(qualifiedName.size() - 1);
				throw newValidationError(snapshot.getTableRef(),
						Static.RESOURCE.notTemporalTable(tableName));
			}
		}
	}

	/**
	 * Validates a namespace.
	 *
	 * @param namespace Namespace
	 * @param targetRowType Desired row type, must not be null, may be the data
	 *                      type 'unknown'.
	 */
	protected void validateNamespace(final SqlValidatorNamespace namespace,
		RelDataType targetRowType) {
		namespace.validate(targetRowType);
		if (namespace.getNode() != null) {
			setValidatedNodeType(namespace.getNode(), namespace.getType());
		}
	}

	@VisibleForTesting
	public SqlValidatorScope getEmptyScope() {
		return new EmptyScope(this);
	}

	public SqlValidatorScope getCursorScope(SqlSelect select) {
		return cursorScopes.get(select);
	}

	public SqlValidatorScope getWhereScope(SqlSelect select) {
		return whereScopes.get(select);
	}

	public SqlValidatorScope getSelectScope(SqlSelect select) {
		return selectScopes.get(select);
	}

	public SelectScope getRawSelectScope(SqlSelect select) {
		SqlValidatorScope scope = getSelectScope(select);
		if (scope instanceof AggregatingSelectScope) {
			scope = ((AggregatingSelectScope) scope).getParent();
		}
		return (SelectScope) scope;
	}

	public SqlValidatorScope getHavingScope(SqlSelect select) {
		// Yes, it's the same as getSelectScope
		return selectScopes.get(select);
	}

	public SqlValidatorScope getGroupScope(SqlSelect select) {
		// Yes, it's the same as getWhereScope
		return groupByScopes.get(select);
	}

	public SqlValidatorScope getFromScope(SqlSelect select) {
		return scopes.get(select);
	}

	public SqlValidatorScope getOrderScope(SqlSelect select) {
		return orderScopes.get(select);
	}

	public SqlValidatorScope getMatchRecognizeScope(SqlMatchRecognize node) {
		return scopes.get(node);
	}

	public SqlValidatorScope getJoinScope(SqlNode node) {
		return scopes.get(stripAs(node));
	}

	public SqlValidatorScope getOverScope(SqlNode node) {
		return scopes.get(node);
	}

	private SqlValidatorNamespace getNamespace(SqlNode node,
		SqlValidatorScope scope) {
		if (node instanceof SqlIdentifier && scope instanceof DelegatingScope) {
			final SqlIdentifier id = (SqlIdentifier) node;
			final DelegatingScope idScope = (DelegatingScope) ((DelegatingScope) scope).getParent();
			return getNamespace(id, idScope);
		} else if (node instanceof SqlCall) {
			// Handle extended identifiers.
			final SqlCall call = (SqlCall) node;
			switch (call.getOperator().getKind()) {
				case EXTEND:
					final SqlIdentifier id = (SqlIdentifier) call.getOperandList().get(0);
					final DelegatingScope idScope = (DelegatingScope) scope;
					return getNamespace(id, idScope);
				case AS:
					final SqlNode nested = call.getOperandList().get(0);
					switch (nested.getKind()) {
						case EXTEND:
							return getNamespace(nested, scope);
					}
					break;
			}
		}
		return getNamespace(node);
	}

	private SqlValidatorNamespace getNamespace(SqlIdentifier id, DelegatingScope scope) {
		if (id.isSimple()) {
			final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
			final SqlValidatorScope.ResolvedImpl resolved =
				new SqlValidatorScope.ResolvedImpl();
			scope.resolve(id.names, nameMatcher, false, resolved);
			if (resolved.count() == 1) {
				return resolved.only().namespace;
			}
		}
		return getNamespace(id);
	}

	public SqlValidatorNamespace getNamespace(SqlNode node) {
		switch (node.getKind()) {
			case AS:

				// AS has a namespace if it has a column list 'AS t (c1, c2, ...)'
				final SqlValidatorNamespace ns = namespaces.get(node);
				if (ns != null) {
					return ns;
				}
				// fall through
			case SNAPSHOT:
			case OVER:
			case COLLECTION_TABLE:
			case ORDER_BY:
			case TABLESAMPLE:
				return getNamespace(((SqlCall) node).operand(0));
			default:
				return namespaces.get(node);
		}
	}

	private void handleOffsetFetch(SqlNode offset, SqlNode fetch) {
		if (offset instanceof SqlDynamicParam) {
			setValidatedNodeType(offset,
				typeFactory.createSqlType(SqlTypeName.INTEGER));
		}
		if (fetch instanceof SqlDynamicParam) {
			setValidatedNodeType(fetch,
				typeFactory.createSqlType(SqlTypeName.INTEGER));
		}
	}

	/**
	 * Performs expression rewrites which are always used unconditionally. These
	 * rewrites massage the expression tree into a standard form so that the
	 * rest of the validation logic can be simpler.
	 *
	 * @param node      expression to be rewritten
	 * @param underFrom whether node appears directly under a FROM clause
	 * @return rewritten expression
	 */
	protected SqlNode performUnconditionalRewrites(
		SqlNode node,
		boolean underFrom) {
		if (node == null) {
			return node;
		}

		SqlNode newOperand;

		// first transform operands and invoke generic call rewrite
		if (node instanceof SqlCall) {
			if (node instanceof SqlMerge) {
				validatingSqlMerge = true;
			}
			SqlCall call = (SqlCall) node;
			final SqlKind kind = call.getKind();
			final List<SqlNode> operands = call.getOperandList();
			for (int i = 0; i < operands.size(); i++) {
				SqlNode operand = operands.get(i);
				boolean childUnderFrom;
				if (kind == SqlKind.SELECT) {
					childUnderFrom = i == SqlSelect.FROM_OPERAND;
				} else if (kind == SqlKind.AS && (i == 0)) {
					// for an aliased expression, it is under FROM if
					// the AS expression is under FROM
					childUnderFrom = underFrom;
				} else {
					childUnderFrom = false;
				}
				newOperand =
					performUnconditionalRewrites(operand, childUnderFrom);
				if (newOperand != null && newOperand != operand) {
					call.setOperand(i, newOperand);
				}
			}

			if (call.getOperator() instanceof SqlUnresolvedFunction) {
				assert call instanceof SqlBasicCall;
				final SqlUnresolvedFunction function =
					(SqlUnresolvedFunction) call.getOperator();
				// This function hasn't been resolved yet.  Perform
				// a half-hearted resolution now in case it's a
				// builtin function requiring special casing.  If it's
				// not, we'll handle it later during overload resolution.
				final List<SqlOperator> overloads = new ArrayList<>();
				opTab.lookupOperatorOverloads(function.getNameAsId(),
						function.getFunctionType(), SqlSyntax.FUNCTION, overloads,
						catalogReader.nameMatcher());
				if (overloads.size() == 1) {
					((SqlBasicCall) call).setOperator(overloads.get(0));
				}
			}
			if (rewriteCalls) {
				node = call.getOperator().rewriteCall(this, call);
			}
		} else if (node instanceof SqlNodeList) {
			SqlNodeList list = (SqlNodeList) node;
			for (int i = 0, count = list.size(); i < count; i++) {
				SqlNode operand = list.get(i);
				newOperand =
					performUnconditionalRewrites(
						operand,
						false);
				if (newOperand != null) {
					list.getList().set(i, newOperand);
				}
			}
		}

		// now transform node itself
		final SqlKind kind = node.getKind();
		switch (kind) {
			case VALUES:
				// CHECKSTYLE: IGNORE 1
				if (underFrom || true) {
					// leave FROM (VALUES(...)) [ AS alias ] clauses alone,
					// otherwise they grow cancerously if this rewrite is invoked
					// over and over
					return node;
				} else {
					final SqlNodeList selectList =
						new SqlNodeList(SqlParserPos.ZERO);
					selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
					return new SqlSelect(node.getParserPosition(), null, selectList, node,
						null, null, null, null, null, null, null);
				}

			case ORDER_BY: {
				SqlOrderBy orderBy = (SqlOrderBy) node;
				handleOffsetFetch(orderBy.offset, orderBy.fetch);
				if (orderBy.query instanceof SqlSelect) {
					SqlSelect select = (SqlSelect) orderBy.query;

					// Don't clobber existing ORDER BY.  It may be needed for
					// an order-sensitive function like RANK.
					if (select.getOrderList() == null) {
						// push ORDER BY into existing select
						select.setOrderBy(orderBy.orderList);
						select.setOffset(orderBy.offset);
						select.setFetch(orderBy.fetch);
						return select;
					}
				}
				if (orderBy.query instanceof SqlWith
					&& ((SqlWith) orderBy.query).body instanceof SqlSelect) {
					SqlWith with = (SqlWith) orderBy.query;
					SqlSelect select = (SqlSelect) with.body;

					// Don't clobber existing ORDER BY.  It may be needed for
					// an order-sensitive function like RANK.
					if (select.getOrderList() == null) {
						// push ORDER BY into existing select
						select.setOrderBy(orderBy.orderList);
						select.setOffset(orderBy.offset);
						select.setFetch(orderBy.fetch);
						return with;
					}
				}
				final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
				selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
				final SqlNodeList orderList;
				if (getInnerSelect(node) != null && isAggregate(getInnerSelect(node))) {
					orderList = SqlNode.clone(orderBy.orderList);
					// We assume that ORDER BY item does not have ASC etc.
					// We assume that ORDER BY item is present in SELECT list.
					for (int i = 0; i < orderList.size(); i++) {
						SqlNode sqlNode = orderList.get(i);
						SqlNodeList selectList2 = getInnerSelect(node).getSelectList();
						for (Ord<SqlNode> sel : Ord.zip(selectList2)) {
							if (stripAs(sel.e).equalsDeep(sqlNode, Litmus.IGNORE)) {
								orderList.set(i,
									SqlLiteral.createExactNumeric(Integer.toString(sel.i + 1),
										SqlParserPos.ZERO));
							}
						}
					}
				} else {
					orderList = orderBy.orderList;
				}
				return new SqlSelect(SqlParserPos.ZERO, null, selectList, orderBy.query,
					null, null, null, null, orderList, orderBy.offset,
					orderBy.fetch);
			}

			case EXPLICIT_TABLE: {
				// (TABLE t) is equivalent to (SELECT * FROM t)
				SqlCall call = (SqlCall) node;
				final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
				selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
				return new SqlSelect(SqlParserPos.ZERO, null, selectList, call.operand(0),
					null, null, null, null, null, null, null);
			}

			case DELETE: {
				SqlDelete call = (SqlDelete) node;
				SqlSelect select = createSourceSelectForDelete(call);
				call.setSourceSelect(select);
				break;
			}

			case UPDATE: {
				SqlUpdate call = (SqlUpdate) node;
				SqlSelect select = createSourceSelectForUpdate(call);
				call.setSourceSelect(select);

				// See if we're supposed to rewrite UPDATE to MERGE
				// (unless this is the UPDATE clause of a MERGE,
				// in which case leave it alone).
				if (!validatingSqlMerge) {
					SqlNode selfJoinSrcExpr =
						getSelfJoinExprForUpdate(
							call.getTargetTable(),
							UPDATE_SRC_ALIAS);
					if (selfJoinSrcExpr != null) {
						node = rewriteUpdateToMerge(call, selfJoinSrcExpr);
					}
				}
				break;
			}

			case MERGE: {
				SqlMerge call = (SqlMerge) node;
				rewriteMerge(call);
				break;
			}
		}
		return node;
	}

	private SqlSelect getInnerSelect(SqlNode node) {
		for (;;) {
			if (node instanceof SqlSelect) {
				return (SqlSelect) node;
			} else if (node instanceof SqlOrderBy) {
				node = ((SqlOrderBy) node).query;
			} else if (node instanceof SqlWith) {
				node = ((SqlWith) node).body;
			} else {
				return null;
			}
		}
	}

	private void rewriteMerge(SqlMerge call) {
		SqlNodeList selectList;
		SqlUpdate updateStmt = call.getUpdateCall();
		if (updateStmt != null) {
			// if we have an update statement, just clone the select list
			// from the update statement's source since it's the same as
			// what we want for the select list of the merge source -- '*'
			// followed by the update set expressions
			selectList = SqlNode.clone(updateStmt.getSourceSelect().getSelectList());
		} else {
			// otherwise, just use select *
			selectList = new SqlNodeList(SqlParserPos.ZERO);
			selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
		}
		SqlNode targetTable = call.getTargetTable();
		if (call.getAlias() != null) {
			targetTable =
				SqlValidatorUtil.addAlias(
					targetTable,
					call.getAlias().getSimple());
		}

		// Provided there is an insert substatement, the source select for
		// the merge is a left outer join between the source in the USING
		// clause and the target table; otherwise, the join is just an
		// inner join.  Need to clone the source table reference in order
		// for validation to work
		SqlNode sourceTableRef = call.getSourceTableRef();
		SqlInsert insertCall = call.getInsertCall();
		JoinType joinType = (insertCall == null) ? JoinType.INNER : JoinType.LEFT;
		final SqlNode leftJoinTerm = SqlNode.clone(sourceTableRef);
		SqlNode outerJoin =
			new SqlJoin(SqlParserPos.ZERO,
				leftJoinTerm,
				SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
				joinType.symbol(SqlParserPos.ZERO),
				targetTable,
				JoinConditionType.ON.symbol(SqlParserPos.ZERO),
				call.getCondition());
		SqlSelect select =
			new SqlSelect(SqlParserPos.ZERO, null, selectList, outerJoin, null,
				null, null, null, null, null, null);
		call.setSourceSelect(select);

		// Source for the insert call is a select of the source table
		// reference with the select list being the value expressions;
		// note that the values clause has already been converted to a
		// select on the values row constructor; so we need to extract
		// that via the from clause on the select
		if (insertCall != null) {
			SqlCall valuesCall = (SqlCall) insertCall.getSource();
			SqlCall rowCall = valuesCall.operand(0);
			selectList =
				new SqlNodeList(
					rowCall.getOperandList(),
					SqlParserPos.ZERO);
			final SqlNode insertSource = SqlNode.clone(sourceTableRef);
			select =
				new SqlSelect(SqlParserPos.ZERO, null, selectList, insertSource, null,
					null, null, null, null, null, null);
			insertCall.setSource(select);
		}
	}

	private SqlNode rewriteUpdateToMerge(
		SqlUpdate updateCall,
		SqlNode selfJoinSrcExpr) {
		// Make sure target has an alias.
		if (updateCall.getAlias() == null) {
			updateCall.setAlias(
				new SqlIdentifier(UPDATE_TGT_ALIAS, SqlParserPos.ZERO));
		}
		SqlNode selfJoinTgtExpr =
			getSelfJoinExprForUpdate(
				updateCall.getTargetTable(),
				updateCall.getAlias().getSimple());
		assert selfJoinTgtExpr != null;

		// Create join condition between source and target exprs,
		// creating a conjunction with the user-level WHERE
		// clause if one was supplied
		SqlNode condition = updateCall.getCondition();
		SqlNode selfJoinCond =
			SqlStdOperatorTable.EQUALS.createCall(
				SqlParserPos.ZERO,
				selfJoinSrcExpr,
				selfJoinTgtExpr);
		if (condition == null) {
			condition = selfJoinCond;
		} else {
			condition =
				SqlStdOperatorTable.AND.createCall(
					SqlParserPos.ZERO,
					selfJoinCond,
					condition);
		}
		SqlNode target =
			updateCall.getTargetTable().clone(SqlParserPos.ZERO);

		// For the source, we need to anonymize the fields, so
		// that for a statement like UPDATE T SET I = I + 1,
		// there's no ambiguity for the "I" in "I + 1";
		// this is OK because the source and target have
		// identical values due to the self-join.
		// Note that we anonymize the source rather than the
		// target because downstream, the optimizer rules
		// don't want to see any projection on top of the target.
		IdentifierNamespace ns =
			new IdentifierNamespace(this, target, null, null);
		RelDataType rowType = ns.getRowType();
		SqlNode source = updateCall.getTargetTable().clone(SqlParserPos.ZERO);
		final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
		int i = 1;
		for (RelDataTypeField field : rowType.getFieldList()) {
			SqlIdentifier col =
				new SqlIdentifier(
					field.getName(),
					SqlParserPos.ZERO);
			selectList.add(
				SqlValidatorUtil.addAlias(col, UPDATE_ANON_PREFIX + i));
			++i;
		}
		source =
			new SqlSelect(SqlParserPos.ZERO, null, selectList, source, null, null,
				null, null, null, null, null);
		source = SqlValidatorUtil.addAlias(source, UPDATE_SRC_ALIAS);
		SqlMerge mergeCall =
			new SqlMerge(updateCall.getParserPosition(), target, condition, source,
				updateCall, null, null, updateCall.getAlias());
		rewriteMerge(mergeCall);
		return mergeCall;
	}

	/**
	 * Allows a subclass to provide information about how to convert an UPDATE
	 * into a MERGE via self-join. If this method returns null, then no such
	 * conversion takes place. Otherwise, this method should return a suitable
	 * unique identifier expression for the given table.
	 *
	 * @param table identifier for table being updated
	 * @param alias alias to use for qualifying columns in expression, or null
	 *              for unqualified references; if this is equal to
	 *              {@value #UPDATE_SRC_ALIAS}, then column references have been
	 *              anonymized to "SYS$ANONx", where x is the 1-based column
	 *              number.
	 * @return expression for unique identifier, or null to prevent conversion
	 */
	protected SqlNode getSelfJoinExprForUpdate(
		SqlNode table,
		String alias) {
		return null;
	}

	/**
	 * Creates the SELECT statement that putatively feeds rows into an UPDATE
	 * statement to be updated.
	 *
	 * @param call Call to the UPDATE operator
	 * @return select statement
	 */
	protected SqlSelect createSourceSelectForUpdate(SqlUpdate call) {
		final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
		selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
		int ordinal = 0;
		for (SqlNode exp : call.getSourceExpressionList()) {
			// Force unique aliases to avoid a duplicate for Y with
			// SET X=Y
			String alias = SqlUtil.deriveAliasFromOrdinal(ordinal);
			selectList.add(SqlValidatorUtil.addAlias(exp, alias));
			++ordinal;
		}
		SqlNode sourceTable = call.getTargetTable();
		if (call.getAlias() != null) {
			sourceTable =
				SqlValidatorUtil.addAlias(
					sourceTable,
					call.getAlias().getSimple());
		}
		return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
			call.getCondition(), null, null, null, null, null, null);
	}

	/**
	 * Creates the SELECT statement that putatively feeds rows into a DELETE
	 * statement to be deleted.
	 *
	 * @param call Call to the DELETE operator
	 * @return select statement
	 */
	protected SqlSelect createSourceSelectForDelete(SqlDelete call) {
		final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
		selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
		SqlNode sourceTable = call.getTargetTable();
		if (call.getAlias() != null) {
			sourceTable =
				SqlValidatorUtil.addAlias(
					sourceTable,
					call.getAlias().getSimple());
		}
		return new SqlSelect(SqlParserPos.ZERO, null, selectList, sourceTable,
			call.getCondition(), null, null, null, null, null, null);
	}

	/**
	 * Returns null if there is no common type. E.g. if the rows have a
	 * different number of columns.
	 */
	RelDataType getTableConstructorRowType(
		SqlCall values,
		SqlValidatorScope scope) {
		final List<SqlNode> rows = values.getOperandList();
		assert rows.size() >= 1;
		final List<RelDataType> rowTypes = new ArrayList<>();
		for (final SqlNode row : rows) {
			assert row.getKind() == SqlKind.ROW;
			SqlCall rowConstructor = (SqlCall) row;

			// REVIEW jvs 10-Sept-2003: Once we support single-row queries as
			// rows, need to infer aliases from there.
			final List<String> aliasList = new ArrayList<>();
			final List<RelDataType> typeList = new ArrayList<>();
			for (Ord<SqlNode> column : Ord.zip(rowConstructor.getOperandList())) {
				final String alias = deriveAlias(column.e, column.i);
				aliasList.add(alias);
				final RelDataType type = deriveType(scope, column.e);
				typeList.add(type);
			}
			rowTypes.add(typeFactory.createStructType(typeList, aliasList));
		}
		if (rows.size() == 1) {
			// TODO jvs 10-Oct-2005:  get rid of this workaround once
			// leastRestrictive can handle all cases
			return rowTypes.get(0);
		}
		return typeFactory.leastRestrictive(rowTypes);
	}

	public RelDataType getValidatedNodeType(SqlNode node) {
		RelDataType type = getValidatedNodeTypeIfKnown(node);
		if (type == null) {
			throw Util.needToImplement(node);
		} else {
			return type;
		}
	}

	public RelDataType getValidatedNodeTypeIfKnown(SqlNode node) {
		final RelDataType type = nodeToTypeMap.get(node);
		if (type != null) {
			return type;
		}
		final SqlValidatorNamespace ns = getNamespace(node);
		if (ns != null) {
			return ns.getType();
		}
		final SqlNode original = originalExprs.get(node);
		if (original != null && original != node) {
			return getValidatedNodeType(original);
		}
		if (node instanceof SqlIdentifier) {
			return getCatalogReader().getNamedType((SqlIdentifier) node);
		}
		return null;
	}

	/**
	 * Saves the type of a {@link SqlNode}, now that it has been validated.
	 *
	 * <p>Unlike the base class method, this method is not deprecated.
	 * It is available from within Calcite, but is not part of the public API.
	 *
	 * @param node A SQL parse tree node, never null
	 * @param type Its type; must not be null
	 */
	@SuppressWarnings("deprecation")
	public final void setValidatedNodeType(SqlNode node, RelDataType type) {
		Objects.requireNonNull(type);
		Objects.requireNonNull(node);
		if (type.equals(unknownType)) {
			// don't set anything until we know what it is, and don't overwrite
			// a known type with the unknown type
			return;
		}
		nodeToTypeMap.put(node, type);
	}

	public void removeValidatedNodeType(SqlNode node) {
		nodeToTypeMap.remove(node);
	}

	@Nullable public SqlCall makeNullaryCall(SqlIdentifier id) {
		if (id.names.size() == 1 && !id.isComponentQuoted(0)) {
			final List<SqlOperator> list = new ArrayList<>();
			opTab.lookupOperatorOverloads(id, null, SqlSyntax.FUNCTION, list,
					catalogReader.nameMatcher());
			for (SqlOperator operator : list) {
				if (operator.getSyntax() == SqlSyntax.FUNCTION_ID) {
					// Even though this looks like an identifier, it is a
					// actually a call to a function. Construct a fake
					// call to this function, so we can use the regular
					// operator validation.
					return new SqlBasicCall(operator, SqlNode.EMPTY_ARRAY,
							id.getParserPosition(), true, null);
				}
			}
		}
		return null;
	}

	public RelDataType deriveType(
		SqlValidatorScope scope,
		SqlNode expr) {
		Objects.requireNonNull(scope);
		Objects.requireNonNull(expr);

		// if we already know the type, no need to re-derive
		RelDataType type = nodeToTypeMap.get(expr);
		if (type != null) {
			return type;
		}
		final SqlValidatorNamespace ns = getNamespace(expr);
		if (ns != null) {
			return ns.getType();
		}
		type = deriveTypeImpl(scope, expr);
		Preconditions.checkArgument(
			type != null,
			"SqlValidator.deriveTypeInternal returned null");
		setValidatedNodeType(expr, type);
		return type;
	}

	/**
	 * Derives the type of a node, never null.
	 */
	RelDataType deriveTypeImpl(
		SqlValidatorScope scope,
		SqlNode operand) {
		DeriveTypeVisitor v = new DeriveTypeVisitor(scope);
		final RelDataType type = operand.accept(v);
		return Objects.requireNonNull(scope.nullifyType(operand, type));
	}

	public RelDataType deriveConstructorType(
		SqlValidatorScope scope,
		SqlCall call,
		SqlFunction unresolvedConstructor,
		SqlFunction resolvedConstructor,
		List<RelDataType> argTypes) {
		SqlIdentifier sqlIdentifier = unresolvedConstructor.getSqlIdentifier();
		assert sqlIdentifier != null;
		RelDataType type = catalogReader.getNamedType(sqlIdentifier);
		if (type == null) {
			// TODO jvs 12-Feb-2005:  proper type name formatting
			throw newValidationError(sqlIdentifier,
				RESOURCE.unknownDatatypeName(sqlIdentifier.toString()));
		}

		if (resolvedConstructor == null) {
			if (call.operandCount() > 0) {
				// This is not a default constructor invocation, and
				// no user-defined constructor could be found
				throw handleUnresolvedFunction(call, unresolvedConstructor, argTypes,
					null);
			}
		} else {
			SqlCall testCall =
				resolvedConstructor.createCall(
					call.getParserPosition(),
					call.getOperandList());
			RelDataType returnType =
				resolvedConstructor.validateOperands(
					this,
					scope,
					testCall);
			assert type == returnType;
		}

		if (shouldExpandIdentifiers()) {
			if (resolvedConstructor != null) {
				((SqlBasicCall) call).setOperator(resolvedConstructor);
			} else {
				// fake a fully-qualified call to the default constructor
				((SqlBasicCall) call).setOperator(
					new SqlFunction(
						type.getSqlIdentifier(),
						ReturnTypes.explicit(type),
						null,
						null,
						null,
						SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR));
			}
		}
		return type;
	}

	public CalciteException handleUnresolvedFunction(SqlCall call,
		SqlFunction unresolvedFunction, List<RelDataType> argTypes,
		List<String> argNames) {
		// For builtins, we can give a better error message
		final List<SqlOperator> overloads = new ArrayList<>();
		opTab.lookupOperatorOverloads(unresolvedFunction.getNameAsId(), null,
				SqlSyntax.FUNCTION, overloads, catalogReader.nameMatcher());
		if (overloads.size() == 1) {
			SqlFunction fun = (SqlFunction) overloads.get(0);
			if ((fun.getSqlIdentifier() == null)
				&& (fun.getSyntax() != SqlSyntax.FUNCTION_ID)) {
				final int expectedArgCount =
					fun.getOperandCountRange().getMin();
				throw newValidationError(call,
					RESOURCE.invalidArgCount(call.getOperator().getName(),
						expectedArgCount));
			}
		}

		AssignableOperandTypeChecker typeChecking =
			new AssignableOperandTypeChecker(argTypes, argNames);
		String signature =
			typeChecking.getAllowedSignatures(
				unresolvedFunction,
				unresolvedFunction.getName());
		throw newValidationError(call,
			RESOURCE.validatorUnknownFunction(signature));
	}

	protected void inferUnknownTypes(
		@Nonnull RelDataType inferredType,
		@Nonnull SqlValidatorScope scope,
		@Nonnull SqlNode node) {
		Objects.requireNonNull(inferredType);
		Objects.requireNonNull(scope);
		Objects.requireNonNull(node);
		final SqlValidatorScope newScope = scopes.get(node);
		if (newScope != null) {
			scope = newScope;
		}
		boolean isNullLiteral = SqlUtil.isNullLiteral(node, false);
		if ((node instanceof SqlDynamicParam) || isNullLiteral) {
			if (inferredType.equals(unknownType)) {
				if (isNullLiteral) {
					throw newValidationError(node, RESOURCE.nullIllegal());
				} else {
					throw newValidationError(node, RESOURCE.dynamicParamIllegal());
				}
			}

			// REVIEW:  should dynamic parameter types always be nullable?
			RelDataType newInferredType =
				typeFactory.createTypeWithNullability(inferredType, true);
			if (SqlTypeUtil.inCharFamily(inferredType)) {
				newInferredType =
					typeFactory.createTypeWithCharsetAndCollation(
						newInferredType,
						inferredType.getCharset(),
						inferredType.getCollation());
			}
			setValidatedNodeType(node, newInferredType);
		} else if (node instanceof SqlNodeList) {
			SqlNodeList nodeList = (SqlNodeList) node;
			if (inferredType.isStruct()) {
				if (inferredType.getFieldCount() != nodeList.size()) {
					// this can happen when we're validating an INSERT
					// where the source and target degrees are different;
					// bust out, and the error will be detected higher up
					return;
				}
			}
			int i = 0;
			for (SqlNode child : nodeList) {
				RelDataType type;
				if (inferredType.isStruct()) {
					type = inferredType.getFieldList().get(i).getType();
					++i;
				} else {
					type = inferredType;
				}
				inferUnknownTypes(type, scope, child);
			}
		} else if (node instanceof SqlCase) {
			final SqlCase caseCall = (SqlCase) node;

			final RelDataType whenType =
				caseCall.getValueOperand() == null ? booleanType : unknownType;
			for (SqlNode sqlNode : caseCall.getWhenOperands().getList()) {
				inferUnknownTypes(whenType, scope, sqlNode);
			}
			RelDataType returnType = deriveType(scope, node);
			for (SqlNode sqlNode : caseCall.getThenOperands().getList()) {
				inferUnknownTypes(returnType, scope, sqlNode);
			}

			if (!SqlUtil.isNullLiteral(caseCall.getElseOperand(), false)) {
				inferUnknownTypes(
					returnType,
					scope,
					caseCall.getElseOperand());
			} else {
				setValidatedNodeType(caseCall.getElseOperand(), returnType);
			}
		} else if (node.getKind()  == SqlKind.AS) {
			// For AS operator, only infer the operand not the alias
			inferUnknownTypes(inferredType, scope, ((SqlCall) node).operand(0));
		} else if (node instanceof SqlCall) {
			final SqlCall call = (SqlCall) node;
			final SqlOperandTypeInference operandTypeInference =
				call.getOperator().getOperandTypeInference();
			final SqlCallBinding callBinding = new SqlCallBinding(this, scope, call);
			final List<SqlNode> operands = callBinding.operands();
			final RelDataType[] operandTypes = new RelDataType[operands.size()];
			Arrays.fill(operandTypes, unknownType);
			// TODO:  eventually should assert(operandTypeInference != null)
			// instead; for now just eat it
			if (operandTypeInference != null) {
				operandTypeInference.inferOperandTypes(
					callBinding,
					inferredType,
					operandTypes);
			}
			for (int i = 0; i < operands.size(); ++i) {
				final SqlNode operand = operands.get(i);
				if (operand != null) {
					inferUnknownTypes(operandTypes[i], scope, operand);
				}
			}
		}
	}

	/**
	 * Adds an expression to a select list, ensuring that its alias does not
	 * clash with any existing expressions on the list.
	 */
	protected void addToSelectList(
		List<SqlNode> list,
		Set<String> aliases,
		List<Map.Entry<String, RelDataType>> fieldList,
		SqlNode exp,
		SqlValidatorScope scope,
		final boolean includeSystemVars) {
		String alias = SqlValidatorUtil.getAlias(exp, -1);
		String uniqueAlias =
			SqlValidatorUtil.uniquify(
				alias, aliases, SqlValidatorUtil.EXPR_SUGGESTER);
		if (!alias.equals(uniqueAlias)) {
			exp = SqlValidatorUtil.addAlias(exp, uniqueAlias);
		}
		fieldList.add(Pair.of(uniqueAlias, deriveType(scope, exp)));
		list.add(exp);
	}

	public String deriveAlias(
		SqlNode node,
		int ordinal) {
		return SqlValidatorUtil.getAlias(node, ordinal);
	}

	// implement SqlValidator
	public void setIdentifierExpansion(boolean expandIdentifiers) {
		this.expandIdentifiers = expandIdentifiers;
	}

	// implement SqlValidator
	public void setColumnReferenceExpansion(
		boolean expandColumnReferences) {
		this.expandColumnReferences = expandColumnReferences;
	}

	// implement SqlValidator
	public boolean getColumnReferenceExpansion() {
		return expandColumnReferences;
	}

	public void setDefaultNullCollation(NullCollation nullCollation) {
		this.nullCollation = Objects.requireNonNull(nullCollation);
	}

	public NullCollation getDefaultNullCollation() {
		return nullCollation;
	}

	// implement SqlValidator
	public void setCallRewrite(boolean rewriteCalls) {
		this.rewriteCalls = rewriteCalls;
	}

	public boolean shouldExpandIdentifiers() {
		return expandIdentifiers;
	}

	protected boolean shouldAllowIntermediateOrderBy() {
		return true;
	}

	private void registerMatchRecognize(
		SqlValidatorScope parentScope,
		SqlValidatorScope usingScope,
		SqlMatchRecognize call,
		SqlNode enclosingNode,
		String alias,
		boolean forceNullable) {

		final MatchRecognizeNamespace matchRecognizeNamespace =
			createMatchRecognizeNameSpace(call, enclosingNode);
		registerNamespace(usingScope, alias, matchRecognizeNamespace, forceNullable);

		final MatchRecognizeScope matchRecognizeScope =
			new MatchRecognizeScope(parentScope, call);
		scopes.put(call, matchRecognizeScope);

		// parse input query
		SqlNode expr = call.getTableRef();
		SqlNode newExpr = registerFrom(usingScope, matchRecognizeScope, true, expr,
			expr, null, null, forceNullable, false);
		if (expr != newExpr) {
			call.setOperand(0, newExpr);
		}
	}

	protected MatchRecognizeNamespace createMatchRecognizeNameSpace(
		SqlMatchRecognize call,
		SqlNode enclosingNode) {
		return new MatchRecognizeNamespace(this, call, enclosingNode);
	}

	/**
	 * Registers a new namespace, and adds it as a child of its parent scope.
	 * Derived class can override this method to tinker with namespaces as they
	 * are created.
	 *
	 * @param usingScope    Parent scope (which will want to look for things in
	 *                      this namespace)
	 * @param alias         Alias by which parent will refer to this namespace
	 * @param ns            Namespace
	 * @param forceNullable Whether to force the type of namespace to be nullable
	 */
	protected void registerNamespace(
		SqlValidatorScope usingScope,
		String alias,
		SqlValidatorNamespace ns,
		boolean forceNullable) {
		namespaces.put(ns.getNode(), ns);
		if (usingScope != null) {
			usingScope.addChild(ns, alias, forceNullable);
		}
	}

	/**
	 * Registers scopes and namespaces implied a relational expression in the
	 * FROM clause.
	 *
	 * <p>{@code parentScope} and {@code usingScope} are often the same. They
	 * differ when the namespace are not visible within the parent. (Example
	 * needed.)
	 *
	 * <p>Likewise, {@code enclosingNode} and {@code node} are often the same.
	 * {@code enclosingNode} is the topmost node within the FROM clause, from
	 * which any decorations like an alias (<code>AS alias</code>) or a table
	 * sample clause are stripped away to get {@code node}. Both are recorded in
	 * the namespace.
	 *
	 * @param parentScope   Parent scope which this scope turns to in order to
	 *                      resolve objects
	 * @param usingScope    Scope whose child list this scope should add itself to
	 * @param register      Whether to register this scope as a child of
	 *                      {@code usingScope}
	 * @param node          Node which namespace is based on
	 * @param enclosingNode Outermost node for namespace, including decorations
	 *                      such as alias and sample clause
	 * @param alias         Alias
	 * @param extendList    Definitions of extended columns
	 * @param forceNullable Whether to force the type of namespace to be
	 *                      nullable because it is in an outer join
	 * @param lateral       Whether LATERAL is specified, so that items to the
	 *                      left of this in the JOIN tree are visible in the
	 *                      scope
	 * @return registered node, usually the same as {@code node}
	 */
	private SqlNode registerFrom(
		SqlValidatorScope parentScope,
		SqlValidatorScope usingScope,
		boolean register,
		final SqlNode node,
		SqlNode enclosingNode,
		String alias,
		SqlNodeList extendList,
		boolean forceNullable,
		final boolean lateral) {
		final SqlKind kind = node.getKind();

		SqlNode expr;
		SqlNode newExpr;

		// Add an alias if necessary.
		SqlNode newNode = node;
		if (alias == null) {
			switch (kind) {
				case IDENTIFIER:
				case OVER:
					alias = deriveAlias(node, -1);
					if (alias == null) {
						alias = deriveAlias(node, nextGeneratedId++);
					}
					if (shouldExpandIdentifiers()) {
						newNode = SqlValidatorUtil.addAlias(node, alias);
					}
					break;

				case SELECT:
				case UNION:
				case INTERSECT:
				case EXCEPT:
				case VALUES:
				case UNNEST:
				case OTHER_FUNCTION:
				case COLLECTION_TABLE:
				case MATCH_RECOGNIZE:

					// give this anonymous construct a name since later
					// query processing stages rely on it
					alias = deriveAlias(node, nextGeneratedId++);
					if (shouldExpandIdentifiers()) {
						// Since we're expanding identifiers, we should make the
						// aliases explicit too, otherwise the expanded query
						// will not be consistent if we convert back to SQL, e.g.
						// "select EXPR$1.EXPR$2 from values (1)".
						newNode = SqlValidatorUtil.addAlias(node, alias);
					}
					break;
			}
		}

		if (lateral) {
			SqlValidatorScope s = usingScope;
			while (s instanceof JoinScope) {
				s = ((JoinScope) s).getUsingScope();
			}
			final SqlNode node2 = s != null ? s.getNode() : node;
			final TableScope tableScope = new TableScope(parentScope, node2);
			if (usingScope instanceof ListScope) {
				for (ScopeChild child : ((ListScope) usingScope).children) {
					tableScope.addChild(child.namespace, child.name, child.nullable);
				}
			}
			parentScope = tableScope;
		}

		SqlCall call;
		SqlNode operand;
		SqlNode newOperand;

		switch (kind) {
			case AS:
				call = (SqlCall) node;
				if (alias == null) {
					alias = call.operand(1).toString();
				}
				final boolean needAlias = call.operandCount() > 2;
				expr = call.operand(0);
				newExpr =
					registerFrom(
						parentScope,
						usingScope,
						!needAlias,
						expr,
						enclosingNode,
						alias,
						extendList,
						forceNullable,
						lateral);
				if (newExpr != expr) {
					call.setOperand(0, newExpr);
				}

				// If alias has a column list, introduce a namespace to translate
				// column names. We skipped registering it just now.
				if (needAlias) {
					registerNamespace(
						usingScope,
						alias,
						new AliasNamespace(this, call, enclosingNode),
						forceNullable);
				}
				return node;
			case MATCH_RECOGNIZE:
				registerMatchRecognize(parentScope, usingScope,
					(SqlMatchRecognize) node, enclosingNode, alias, forceNullable);
				return node;
			case TABLESAMPLE:
				call = (SqlCall) node;
				expr = call.operand(0);
				newExpr =
					registerFrom(
						parentScope,
						usingScope,
						true,
						expr,
						enclosingNode,
						alias,
						extendList,
						forceNullable,
						lateral);
				if (newExpr != expr) {
					call.setOperand(0, newExpr);
				}
				return node;

			case JOIN:
				final SqlJoin join = (SqlJoin) node;
				final JoinScope joinScope =
					new JoinScope(parentScope, usingScope, join);
				scopes.put(join, joinScope);
				final SqlNode left = join.getLeft();
				final SqlNode right = join.getRight();
				final boolean rightIsLateral = isLateral(right);
				boolean forceLeftNullable = forceNullable;
				boolean forceRightNullable = forceNullable;
				switch (join.getJoinType()) {
					case LEFT:
						forceRightNullable = true;
						break;
					case RIGHT:
						forceLeftNullable = true;
						break;
					case FULL:
						forceLeftNullable = true;
						forceRightNullable = true;
						break;
				}
				final SqlNode newLeft =
					registerFrom(
						parentScope,
						joinScope,
						true,
						left,
						left,
						null,
						null,
						forceLeftNullable,
						lateral);
				if (newLeft != left) {
					join.setLeft(newLeft);
				}
				final SqlNode newRight =
					registerFrom(
						parentScope,
						joinScope,
						true,
						right,
						right,
						null,
						null,
						forceRightNullable,
						lateral);
				if (newRight != right) {
					join.setRight(newRight);
				}
				registerSubQueries(joinScope, join.getCondition());
				final JoinNamespace joinNamespace = new JoinNamespace(this, join);
				registerNamespace(null, null, joinNamespace, forceNullable);
				return join;

			case IDENTIFIER:
				final SqlIdentifier id = (SqlIdentifier) node;
				final IdentifierNamespace newNs =
					new IdentifierNamespace(
						this, id, extendList, enclosingNode,
						parentScope);
				registerNamespace(register ? usingScope : null, alias, newNs,
					forceNullable);
				if (tableScope == null) {
					tableScope = new TableScope(parentScope, node);
				}
				tableScope.addChild(newNs, alias, forceNullable);
				if (extendList != null && extendList.size() != 0) {
					return enclosingNode;
				}
				return newNode;

			case LATERAL:
				return registerFrom(
					parentScope,
					usingScope,
					register,
					((SqlCall) node).operand(0),
					enclosingNode,
					alias,
					extendList,
					forceNullable,
					true);

			case COLLECTION_TABLE:
				call = (SqlCall) node;
				operand = call.operand(0);
				newOperand =
					registerFrom(
						parentScope,
						usingScope,
						register,
						operand,
						enclosingNode,
						alias,
						extendList,
						forceNullable, lateral);
				if (newOperand != operand) {
					call.setOperand(0, newOperand);
				}
				scopes.put(node, parentScope);
				return newNode;

			case UNNEST:
				if (!lateral) {
					return registerFrom(parentScope, usingScope, register, node,
						enclosingNode, alias, extendList, forceNullable, true);
				}
				// fall through
			case SELECT:
			case UNION:
			case INTERSECT:
			case EXCEPT:
			case VALUES:
			case WITH:
			case OTHER_FUNCTION:
				if (alias == null) {
					alias = deriveAlias(node, nextGeneratedId++);
				}
				registerQuery(
					parentScope,
					register ? usingScope : null,
					node,
					enclosingNode,
					alias,
					forceNullable);
				return newNode;

			case OVER:
				if (!shouldAllowOverRelation()) {
					throw Util.unexpected(kind);
				}
				call = (SqlCall) node;
				final OverScope overScope = new OverScope(usingScope, call);
				scopes.put(call, overScope);
				operand = call.operand(0);
				newOperand =
					registerFrom(
						parentScope,
						overScope,
						true,
						operand,
						enclosingNode,
						alias,
						extendList,
						forceNullable,
						lateral);
				if (newOperand != operand) {
					call.setOperand(0, newOperand);
				}

				for (ScopeChild child : overScope.children) {
					registerNamespace(register ? usingScope : null, child.name,
						child.namespace, forceNullable);
				}

				return newNode;

			case EXTEND:
				final SqlCall extend = (SqlCall) node;
				return registerFrom(parentScope,
					usingScope,
					true,
					extend.getOperandList().get(0),
					extend,
					alias,
					(SqlNodeList) extend.getOperandList().get(1),
					forceNullable,
					lateral);

			case SNAPSHOT:
				call = (SqlCall) node;
				operand = call.operand(0);
				newOperand = registerFrom(
						tableScope == null ? parentScope : tableScope,
						usingScope,
						register,
						operand,
						enclosingNode,
						alias,
						extendList,
						forceNullable,
						true);
				if (newOperand != operand) {
					call.setOperand(0, newOperand);
				}
				scopes.put(node, parentScope);
				return newNode;

			default:
				throw Util.unexpected(kind);
		}
	}

	private static boolean isLateral(SqlNode node) {
		switch (node.getKind()) {
			case LATERAL:
			case UNNEST:
				// Per SQL std, UNNEST is implicitly LATERAL.
				return true;
			case AS:
				return isLateral(((SqlCall) node).operand(0));
			default:
				return false;
		}
	}

	protected boolean shouldAllowOverRelation() {
		return false;
	}

	/**
	 * Creates a namespace for a <code>SELECT</code> node. Derived class may
	 * override this factory method.
	 *
	 * @param select        Select node
	 * @param enclosingNode Enclosing node
	 * @return Select namespace
	 */
	protected SelectNamespace createSelectNamespace(
		SqlSelect select,
		SqlNode enclosingNode) {
		return new SelectNamespace(this, select, enclosingNode);
	}

	/**
	 * Creates a namespace for a set operation (<code>UNION</code>, <code>
	 * INTERSECT</code>, or <code>EXCEPT</code>). Derived class may override
	 * this factory method.
	 *
	 * @param call          Call to set operation
	 * @param enclosingNode Enclosing node
	 * @return Set operation namespace
	 */
	protected SetopNamespace createSetopNamespace(
		SqlCall call,
		SqlNode enclosingNode) {
		return new SetopNamespace(this, call, enclosingNode);
	}

	/**
	 * Registers a query in a parent scope.
	 *
	 * @param parentScope Parent scope which this scope turns to in order to
	 *                    resolve objects
	 * @param usingScope  Scope whose child list this scope should add itself to
	 * @param node        Query node
	 * @param alias       Name of this query within its parent. Must be specified
	 *                    if usingScope != null
	 */
	private void registerQuery(
		SqlValidatorScope parentScope,
		SqlValidatorScope usingScope,
		SqlNode node,
		SqlNode enclosingNode,
		String alias,
		boolean forceNullable) {
		Preconditions.checkArgument(usingScope == null || alias != null);
		registerQuery(
			parentScope,
			usingScope,
			node,
			enclosingNode,
			alias,
			forceNullable,
			true);
	}

	/**
	 * Registers a query in a parent scope.
	 *
	 * @param parentScope Parent scope which this scope turns to in order to
	 *                    resolve objects
	 * @param usingScope  Scope whose child list this scope should add itself to
	 * @param node        Query node
	 * @param alias       Name of this query within its parent. Must be specified
	 *                    if usingScope != null
	 * @param checkUpdate if true, validate that the update feature is supported
	 *                    if validating the update statement
	 */
	private void registerQuery(
		SqlValidatorScope parentScope,
		SqlValidatorScope usingScope,
		SqlNode node,
		SqlNode enclosingNode,
		String alias,
		boolean forceNullable,
		boolean checkUpdate) {
		Objects.requireNonNull(node);
		Objects.requireNonNull(enclosingNode);
		Preconditions.checkArgument(usingScope == null || alias != null);

		SqlCall call;
		List<SqlNode> operands;
		switch (node.getKind()) {
			case SELECT:
				final SqlSelect select = (SqlSelect) node;
				final SelectNamespace selectNs =
					createSelectNamespace(select, enclosingNode);
				registerNamespace(usingScope, alias, selectNs, forceNullable);
				final SqlValidatorScope windowParentScope =
					(usingScope != null) ? usingScope : parentScope;
				SelectScope selectScope =
					new SelectScope(parentScope, windowParentScope, select);
				scopes.put(select, selectScope);

				// Start by registering the WHERE clause
				whereScopes.put(select, selectScope);
				registerOperandSubQueries(
					selectScope,
					select,
					SqlSelect.WHERE_OPERAND);

				// Register FROM with the inherited scope 'parentScope', not
				// 'selectScope', otherwise tables in the FROM clause would be
				// able to see each other.
				final SqlNode from = select.getFrom();
				if (from != null) {
					final SqlNode newFrom =
						registerFrom(
							parentScope,
							selectScope,
							true,
							from,
							from,
							null,
							null,
							false,
							false);
					if (newFrom != from) {
						select.setFrom(newFrom);
					}
				}

				// If this is an aggregating query, the SELECT list and HAVING
				// clause use a different scope, where you can only reference
				// columns which are in the GROUP BY clause.
				SqlValidatorScope aggScope = selectScope;
				if (isAggregate(select)) {
					aggScope =
						new AggregatingSelectScope(selectScope, select, false);
					selectScopes.put(select, aggScope);
				} else {
					selectScopes.put(select, selectScope);
				}
				if (select.getGroup() != null) {
					GroupByScope groupByScope =
						new GroupByScope(selectScope, select.getGroup(), select);
					groupByScopes.put(select, groupByScope);
					registerSubQueries(groupByScope, select.getGroup());
				}
				registerOperandSubQueries(
					aggScope,
					select,
					SqlSelect.HAVING_OPERAND);
				registerSubQueries(aggScope, select.getSelectList());
				final SqlNodeList orderList = select.getOrderList();
				if (orderList != null) {
					// If the query is 'SELECT DISTINCT', restrict the columns
					// available to the ORDER BY clause.
					if (select.isDistinct()) {
						aggScope =
							new AggregatingSelectScope(selectScope, select, true);
					}
					OrderByScope orderScope =
						new OrderByScope(aggScope, orderList, select);
					orderScopes.put(select, orderScope);
					registerSubQueries(orderScope, orderList);

					if (!isAggregate(select)) {
						// Since this is not an aggregating query,
						// there cannot be any aggregates in the ORDER BY clause.
						SqlNode agg = aggFinder.findAgg(orderList);
						if (agg != null) {
							throw newValidationError(agg, RESOURCE.aggregateIllegalInOrderBy());
						}
					}
				}
				break;

			case INTERSECT:
				validateFeature(RESOURCE.sQLFeature_F302(), node.getParserPosition());
				registerSetop(
					parentScope,
					usingScope,
					node,
					node,
					alias,
					forceNullable);
				break;

			case EXCEPT:
				validateFeature(RESOURCE.sQLFeature_E071_03(), node.getParserPosition());
				registerSetop(
					parentScope,
					usingScope,
					node,
					node,
					alias,
					forceNullable);
				break;

			case UNION:
				registerSetop(
					parentScope,
					usingScope,
					node,
					node,
					alias,
					forceNullable);
				break;

			case WITH:
				registerWith(parentScope, usingScope, (SqlWith) node, enclosingNode,
					alias, forceNullable, checkUpdate);
				break;

			case VALUES:
				call = (SqlCall) node;
				scopes.put(call, parentScope);
				final TableConstructorNamespace tableConstructorNamespace =
					new TableConstructorNamespace(
						this,
						call,
						parentScope,
						enclosingNode);
				registerNamespace(
					usingScope,
					alias,
					tableConstructorNamespace,
					forceNullable);
				operands = call.getOperandList();
				for (int i = 0; i < operands.size(); ++i) {
					assert operands.get(i).getKind() == SqlKind.ROW;

					// FIXME jvs 9-Feb-2005:  Correlation should
					// be illegal in these sub-queries.  Same goes for
					// any non-lateral SELECT in the FROM list.
					registerOperandSubQueries(parentScope, call, i);
				}
				break;

			case INSERT:
				SqlInsert insertCall = (SqlInsert) node;
				InsertNamespace insertNs =
					new InsertNamespace(
						this,
						insertCall,
						enclosingNode,
						parentScope);
				registerNamespace(usingScope, null, insertNs, forceNullable);
				registerQuery(
					parentScope,
					usingScope,
					insertCall.getSource(),
					enclosingNode,
					null,
					false);
				break;

			case DELETE:
				SqlDelete deleteCall = (SqlDelete) node;
				DeleteNamespace deleteNs =
					new DeleteNamespace(
						this,
						deleteCall,
						enclosingNode,
						parentScope);
				registerNamespace(usingScope, null, deleteNs, forceNullable);
				registerQuery(
					parentScope,
					usingScope,
					deleteCall.getSourceSelect(),
					enclosingNode,
					null,
					false);
				break;

			case UPDATE:
				if (checkUpdate) {
					validateFeature(RESOURCE.sQLFeature_E101_03(),
						node.getParserPosition());
				}
				SqlUpdate updateCall = (SqlUpdate) node;
				UpdateNamespace updateNs =
					new UpdateNamespace(
						this,
						updateCall,
						enclosingNode,
						parentScope);
				registerNamespace(usingScope, null, updateNs, forceNullable);
				registerQuery(
					parentScope,
					usingScope,
					updateCall.getSourceSelect(),
					enclosingNode,
					null,
					false);
				break;

			case MERGE:
				validateFeature(RESOURCE.sQLFeature_F312(), node.getParserPosition());
				SqlMerge mergeCall = (SqlMerge) node;
				MergeNamespace mergeNs =
					new MergeNamespace(
						this,
						mergeCall,
						enclosingNode,
						parentScope);
				registerNamespace(usingScope, null, mergeNs, forceNullable);
				registerQuery(
					parentScope,
					usingScope,
					mergeCall.getSourceSelect(),
					enclosingNode,
					null,
					false);

				// update call can reference either the source table reference
				// or the target table, so set its parent scope to the merge's
				// source select; when validating the update, skip the feature
				// validation check
				if (mergeCall.getUpdateCall() != null) {
					registerQuery(
						whereScopes.get(mergeCall.getSourceSelect()),
						null,
						mergeCall.getUpdateCall(),
						enclosingNode,
						null,
						false,
						false);
				}
				if (mergeCall.getInsertCall() != null) {
					registerQuery(
						parentScope,
						null,
						mergeCall.getInsertCall(),
						enclosingNode,
						null,
						false);
				}
				break;

			case UNNEST:
				call = (SqlCall) node;
				final UnnestNamespace unnestNs =
					new UnnestNamespace(this, call, parentScope, enclosingNode);
				registerNamespace(
					usingScope,
					alias,
					unnestNs,
					forceNullable);
				registerOperandSubQueries(parentScope, call, 0);
				scopes.put(node, parentScope);
				break;

			case OTHER_FUNCTION:
				call = (SqlCall) node;
				ProcedureNamespace procNs =
					new ProcedureNamespace(
						this,
						parentScope,
						call,
						enclosingNode);
				registerNamespace(
					usingScope,
					alias,
					procNs,
					forceNullable);
				registerSubQueries(parentScope, call);
				break;

			case MULTISET_QUERY_CONSTRUCTOR:
			case MULTISET_VALUE_CONSTRUCTOR:
				validateFeature(RESOURCE.sQLFeature_S271(), node.getParserPosition());
				call = (SqlCall) node;
				CollectScope cs = new CollectScope(parentScope, usingScope, call);
				final CollectNamespace tableConstructorNs =
					new CollectNamespace(call, cs, enclosingNode);
				final String alias2 = deriveAlias(node, nextGeneratedId++);
				registerNamespace(
					usingScope,
					alias2,
					tableConstructorNs,
					forceNullable);
				operands = call.getOperandList();
				for (int i = 0; i < operands.size(); i++) {
					registerOperandSubQueries(parentScope, call, i);
				}
				break;

			default:
				throw Util.unexpected(node.getKind());
		}
	}

	private void registerSetop(
		SqlValidatorScope parentScope,
		SqlValidatorScope usingScope,
		SqlNode node,
		SqlNode enclosingNode,
		String alias,
		boolean forceNullable) {
		SqlCall call = (SqlCall) node;
		final SetopNamespace setopNamespace =
			createSetopNamespace(call, enclosingNode);
		registerNamespace(usingScope, alias, setopNamespace, forceNullable);

		// A setop is in the same scope as its parent.
		scopes.put(call, parentScope);
		for (SqlNode operand : call.getOperandList()) {
			registerQuery(
				parentScope,
				null,
				operand,
				operand,
				null,
				false);
		}
	}

	private void registerWith(
		SqlValidatorScope parentScope,
		SqlValidatorScope usingScope,
		SqlWith with,
		SqlNode enclosingNode,
		String alias,
		boolean forceNullable,
		boolean checkUpdate) {
		final WithNamespace withNamespace =
			new WithNamespace(this, with, enclosingNode);
		registerNamespace(usingScope, alias, withNamespace, forceNullable);

		SqlValidatorScope scope = parentScope;
		for (SqlNode withItem_ : with.withList) {
			final SqlWithItem withItem = (SqlWithItem) withItem_;
			final WithScope withScope = new WithScope(scope, withItem);
			scopes.put(withItem, withScope);

			registerQuery(scope, null, withItem.query, with,
				withItem.name.getSimple(), false);
			registerNamespace(null, alias,
				new WithItemNamespace(this, withItem, enclosingNode),
				false);
			scope = withScope;
		}

		registerQuery(scope, null, with.body, enclosingNode, alias, forceNullable,
			checkUpdate);
	}

	public boolean isAggregate(SqlSelect select) {
		if (getAggregate(select) != null) {
			return true;
		}
		// Also when nested window aggregates are present
		for (SqlCall call : overFinder.findAll(select.getSelectList())) {
			assert call.getKind() == SqlKind.OVER;
			if (isNestedAggregateWindow(call.operand(0))) {
				return true;
			}
			if (isOverAggregateWindow(call.operand(1))) {
				return true;
			}
		}
		return false;
	}

	protected boolean isNestedAggregateWindow(SqlNode node) {
		AggFinder nestedAggFinder =
				new AggFinder(opTab, false, false, false, aggFinder,
						catalogReader.nameMatcher());
		return nestedAggFinder.findAgg(node) != null;
	}

	protected boolean isOverAggregateWindow(SqlNode node) {
		return aggFinder.findAgg(node) != null;
	}

	/** Returns the parse tree node (GROUP BY, HAVING, or an aggregate function
	 * call) that causes {@code select} to be an aggregate query, or null if it
	 * is not an aggregate query.
	 *
	 * <p>The node is useful context for error messages,
	 * but you cannot assume that the node is the only aggregate function. */
	protected SqlNode getAggregate(SqlSelect select) {
		SqlNode node = select.getGroup();
		if (node != null) {
			return node;
		}
		node = select.getHaving();
		if (node != null) {
			return node;
		}
		return getAgg(select);
	}

	/** If there is at least one call to an aggregate function, returns the
	 * first. */
	private SqlNode getAgg(SqlSelect select) {
		final SelectScope selectScope = getRawSelectScope(select);
		if (selectScope != null) {
			final List<SqlNode> selectList = selectScope.getExpandedSelectList();
			if (selectList != null) {
				return aggFinder.findAgg(selectList);
			}
		}
		return aggFinder.findAgg(select.getSelectList());
	}

	@SuppressWarnings("deprecation")
	public boolean isAggregate(SqlNode selectNode) {
		return aggFinder.findAgg(selectNode) != null;
	}

	private void validateNodeFeature(SqlNode node) {
		switch (node.getKind()) {
			case MULTISET_VALUE_CONSTRUCTOR:
				validateFeature(RESOURCE.sQLFeature_S271(), node.getParserPosition());
				break;
		}
	}

	private void registerSubQueries(
		SqlValidatorScope parentScope,
		SqlNode node) {
		if (node == null) {
			return;
		}
		if (node.getKind().belongsTo(SqlKind.QUERY)
			|| node.getKind() == SqlKind.MULTISET_QUERY_CONSTRUCTOR
			|| node.getKind() == SqlKind.MULTISET_VALUE_CONSTRUCTOR) {
			registerQuery(parentScope, null, node, node, null, false);
		} else if (node instanceof SqlCall) {
			validateNodeFeature(node);
			SqlCall call = (SqlCall) node;
			for (int i = 0; i < call.operandCount(); i++) {
				registerOperandSubQueries(parentScope, call, i);
			}
		} else if (node instanceof SqlNodeList) {
			SqlNodeList list = (SqlNodeList) node;
			for (int i = 0, count = list.size(); i < count; i++) {
				SqlNode listNode = list.get(i);
				if (listNode.getKind().belongsTo(SqlKind.QUERY)) {
					listNode =
						SqlStdOperatorTable.SCALAR_QUERY.createCall(
							listNode.getParserPosition(),
							listNode);
					list.set(i, listNode);
				}
				registerSubQueries(parentScope, listNode);
			}
		} else {
			// atomic node -- can be ignored
		}
	}

	/**
	 * Registers any sub-queries inside a given call operand, and converts the
	 * operand to a scalar sub-query if the operator requires it.
	 *
	 * @param parentScope    Parent scope
	 * @param call           Call
	 * @param operandOrdinal Ordinal of operand within call
	 * @see SqlOperator#argumentMustBeScalar(int)
	 */
	private void registerOperandSubQueries(
		SqlValidatorScope parentScope,
		SqlCall call,
		int operandOrdinal) {
		SqlNode operand = call.operand(operandOrdinal);
		if (operand == null) {
			return;
		}
		if (operand.getKind().belongsTo(SqlKind.QUERY)
			&& call.getOperator().argumentMustBeScalar(operandOrdinal)) {
			operand =
				SqlStdOperatorTable.SCALAR_QUERY.createCall(
					operand.getParserPosition(),
					operand);
			call.setOperand(operandOrdinal, operand);
		}
		registerSubQueries(parentScope, operand);
	}

	public void validateIdentifier(SqlIdentifier id, SqlValidatorScope scope) {
		final SqlQualified fqId = scope.fullyQualify(id);
		if (expandColumnReferences) {
			// NOTE jvs 9-Apr-2007: this doesn't cover ORDER BY, which has its
			// own ideas about qualification.
			id.assignNamesFrom(fqId.identifier);
		} else {
			Util.discard(fqId);
		}
	}

	public void validateLiteral(SqlLiteral literal) {
		switch (literal.getTypeName()) {
			case DECIMAL:
				// Decimal and long have the same precision (as 64-bit integers), so
				// the unscaled value of a decimal must fit into a long.

				// REVIEW jvs 4-Aug-2004:  This should probably be calling over to
				// the available calculator implementations to see what they
				// support.  For now use ESP instead.
				//
				// jhyde 2006/12/21: I think the limits should be baked into the
				// type system, not dependent on the calculator implementation.
				BigDecimal bd = (BigDecimal) literal.getValue();
				BigInteger unscaled = bd.unscaledValue();
				long longValue = unscaled.longValue();
				if (!BigInteger.valueOf(longValue).equals(unscaled)) {
					// overflow
					throw newValidationError(literal,
						RESOURCE.numberLiteralOutOfRange(bd.toString()));
				}
				break;

			case DOUBLE:
				validateLiteralAsDouble(literal);
				break;

			case BINARY:
				final BitString bitString = (BitString) literal.getValue();
				if ((bitString.getBitCount() % 8) != 0) {
					throw newValidationError(literal, RESOURCE.binaryLiteralOdd());
				}
				break;

			case DATE:
			case TIME:
			case TIMESTAMP:
				Calendar calendar = literal.getValueAs(Calendar.class);
				final int year = calendar.get(Calendar.YEAR);
				final int era = calendar.get(Calendar.ERA);
				if (year < 1 || era == GregorianCalendar.BC || year > 9999) {
					throw newValidationError(literal,
						RESOURCE.dateLiteralOutOfRange(literal.toString()));
				}
				break;

			case INTERVAL_YEAR:
			case INTERVAL_YEAR_MONTH:
			case INTERVAL_MONTH:
			case INTERVAL_DAY:
			case INTERVAL_DAY_HOUR:
			case INTERVAL_DAY_MINUTE:
			case INTERVAL_DAY_SECOND:
			case INTERVAL_HOUR:
			case INTERVAL_HOUR_MINUTE:
			case INTERVAL_HOUR_SECOND:
			case INTERVAL_MINUTE:
			case INTERVAL_MINUTE_SECOND:
			case INTERVAL_SECOND:
				if (literal instanceof SqlIntervalLiteral) {
					SqlIntervalLiteral.IntervalValue interval =
						(SqlIntervalLiteral.IntervalValue)
							literal.getValue();
					SqlIntervalQualifier intervalQualifier =
						interval.getIntervalQualifier();

					// ensure qualifier is good before attempting to validate literal
					validateIntervalQualifier(intervalQualifier);
					String intervalStr = interval.getIntervalLiteral();
					// throws CalciteContextException if string is invalid
					int[] values = intervalQualifier.evaluateIntervalLiteral(intervalStr,
						literal.getParserPosition(), typeFactory.getTypeSystem());
					Util.discard(values);
				}
				break;
			default:
				// default is to do nothing
		}
	}

	private void validateLiteralAsDouble(SqlLiteral literal) {
		BigDecimal bd = (BigDecimal) literal.getValue();
		double d = bd.doubleValue();
		if (Double.isInfinite(d) || Double.isNaN(d)) {
			// overflow
			throw newValidationError(literal,
				RESOURCE.numberLiteralOutOfRange(Util.toScientificNotation(bd)));
		}

		// REVIEW jvs 4-Aug-2004:  what about underflow?
	}

	public void validateIntervalQualifier(SqlIntervalQualifier qualifier) {
		assert qualifier != null;
		boolean startPrecisionOutOfRange = false;
		boolean fractionalSecondPrecisionOutOfRange = false;
		final RelDataTypeSystem typeSystem = typeFactory.getTypeSystem();

		final int startPrecision = qualifier.getStartPrecision(typeSystem);
		final int fracPrecision =
			qualifier.getFractionalSecondPrecision(typeSystem);
		final int maxPrecision = typeSystem.getMaxPrecision(qualifier.typeName());
		final int minPrecision = qualifier.typeName().getMinPrecision();
		final int minScale = qualifier.typeName().getMinScale();
		final int maxScale = typeSystem.getMaxScale(qualifier.typeName());
		if (qualifier.isYearMonth()) {
			if (startPrecision < minPrecision || startPrecision > maxPrecision) {
				startPrecisionOutOfRange = true;
			} else {
				if (fracPrecision < minScale || fracPrecision > maxScale) {
					fractionalSecondPrecisionOutOfRange = true;
				}
			}
		} else {
			if (startPrecision < minPrecision || startPrecision > maxPrecision) {
				startPrecisionOutOfRange = true;
			} else {
				if (fracPrecision < minScale || fracPrecision > maxScale) {
					fractionalSecondPrecisionOutOfRange = true;
				}
			}
		}

		if (startPrecisionOutOfRange) {
			throw newValidationError(qualifier,
				RESOURCE.intervalStartPrecisionOutOfRange(startPrecision,
					"INTERVAL " + qualifier));
		} else if (fractionalSecondPrecisionOutOfRange) {
			throw newValidationError(qualifier,
				RESOURCE.intervalFractionalSecondPrecisionOutOfRange(
					fracPrecision,
					"INTERVAL " + qualifier));
		}
	}

	/**
	 * Validates the FROM clause of a query, or (recursively) a child node of
	 * the FROM clause: AS, OVER, JOIN, VALUES, or sub-query.
	 *
	 * @param node          Node in FROM clause, typically a table or derived
	 *                      table
	 * @param targetRowType Desired row type of this expression, or
	 *                      {@link #unknownType} if not fussy. Must not be null.
	 * @param scope         Scope
	 */
	protected void validateFrom(
		SqlNode node,
		RelDataType targetRowType,
		SqlValidatorScope scope) {
		Objects.requireNonNull(targetRowType);
		switch (node.getKind()) {
			case AS:
				validateFrom(
					((SqlCall) node).operand(0),
					targetRowType,
					scope);
				break;
			case VALUES:
				validateValues((SqlCall) node, targetRowType, scope);
				break;
			case JOIN:
				validateJoin((SqlJoin) node, scope);
				break;
			case OVER:
				validateOver((SqlCall) node, scope);
				break;
			case UNNEST:
				validateUnnest((SqlCall) node, scope, targetRowType);
				break;
			default:
				validateQuery(node, scope, targetRowType);
				break;
		}

		// Validate the namespace representation of the node, just in case the
		// validation did not occur implicitly.
		getNamespace(node, scope).validate(targetRowType);
	}

	protected void validateOver(SqlCall call, SqlValidatorScope scope) {
		throw new AssertionError("OVER unexpected in this context");
	}

	protected void validateUnnest(SqlCall call, SqlValidatorScope scope, RelDataType targetRowType) {
		for (int i = 0; i < call.operandCount(); i++) {
			SqlNode expandedItem = expand(call.operand(i), scope);
			call.setOperand(i, expandedItem);
		}
		validateQuery(call, scope, targetRowType);
	}

	private void checkRollUpInUsing(SqlIdentifier identifier,
			SqlNode leftOrRight, SqlValidatorScope scope) {
		SqlValidatorNamespace namespace = getNamespace(leftOrRight, scope);
		if (namespace != null) {
			SqlValidatorTable sqlValidatorTable = namespace.getTable();
			if (sqlValidatorTable != null) {
				Table table = sqlValidatorTable.unwrap(Table.class);
				String column = Util.last(identifier.names);

				if (table.isRolledUp(column)) {
					throw newValidationError(identifier,
							RESOURCE.rolledUpNotAllowed(column, "USING"));
				}
			}
		}
	}

	protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
		SqlNode left = join.getLeft();
		SqlNode right = join.getRight();
		SqlNode condition = join.getCondition();
		boolean natural = join.isNatural();
		final JoinType joinType = join.getJoinType();
		final JoinConditionType conditionType = join.getConditionType();
		final SqlValidatorScope joinScope = scopes.get(join);
		validateFrom(left, unknownType, joinScope);
		validateFrom(right, unknownType, joinScope);

		// Validate condition.
		switch (conditionType) {
			case NONE:
				Preconditions.checkArgument(condition == null);
				break;
			case ON:
				Preconditions.checkArgument(condition != null);
				SqlNode expandedCondition = expand(condition, joinScope);
				join.setOperand(5, expandedCondition);
				condition = join.getCondition();
				validateWhereOrOn(joinScope, condition, "ON");
				checkRollUp(null, join, condition, joinScope, "ON");
				break;
			case USING:
				SqlNodeList list = (SqlNodeList) condition;

				// Parser ensures that using clause is not empty.
				Preconditions.checkArgument(list.size() > 0, "Empty USING clause");
				for (SqlNode node : list) {
					SqlIdentifier id = (SqlIdentifier) node;
					final RelDataType leftColType = validateUsingCol(id, left);
					final RelDataType rightColType = validateUsingCol(id, right);
					if (!SqlTypeUtil.isComparable(leftColType, rightColType)) {
						throw newValidationError(id,
							RESOURCE.naturalOrUsingColumnNotCompatible(id.getSimple(),
								leftColType.toString(), rightColType.toString()));
					}
					checkRollUpInUsing(id, left, scope);
					checkRollUpInUsing(id, right, scope);
				}
				break;
			default:
				throw Util.unexpected(conditionType);
		}

		// Validate NATURAL.
		if (natural) {
			if (condition != null) {
				throw newValidationError(condition,
					RESOURCE.naturalDisallowsOnOrUsing());
			}

			// Join on fields that occur exactly once on each side. Ignore
			// fields that occur more than once on either side.
			final RelDataType leftRowType = getNamespace(left).getRowType();
			final RelDataType rightRowType = getNamespace(right).getRowType();
			final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
			List<String> naturalColumnNames =
				SqlValidatorUtil.deriveNaturalJoinColumnList(nameMatcher,
					leftRowType, rightRowType);

			// Check compatibility of the chosen columns.
			for (String name : naturalColumnNames) {
				final RelDataType leftColType =
					nameMatcher.field(leftRowType, name).getType();
				final RelDataType rightColType =
					nameMatcher.field(rightRowType, name).getType();
				if (!SqlTypeUtil.isComparable(leftColType, rightColType)) {
					throw newValidationError(join,
						RESOURCE.naturalOrUsingColumnNotCompatible(name,
							leftColType.toString(), rightColType.toString()));
				}
			}
		}

		// Which join types require/allow a ON/USING condition, or allow
		// a NATURAL keyword?
		switch (joinType) {
			case LEFT_SEMI_JOIN:
				if (!conformance.isLiberal()) {
					throw newValidationError(join.getJoinTypeNode(),
						RESOURCE.dialectDoesNotSupportFeature("LEFT SEMI JOIN"));
				}
				// fall through
			case INNER:
			case LEFT:
			case RIGHT:
			case FULL:
				if ((condition == null) && !natural) {
					throw newValidationError(join, RESOURCE.joinRequiresCondition());
				}
				break;
			case COMMA:
			case CROSS:
				if (condition != null) {
					throw newValidationError(join.getConditionTypeNode(),
						RESOURCE.crossJoinDisallowsCondition());
				}
				if (natural) {
					throw newValidationError(join.getConditionTypeNode(),
						RESOURCE.crossJoinDisallowsCondition());
				}
				break;
			default:
				throw Util.unexpected(joinType);
		}
	}

	/**
	 * Throws an error if there is an aggregate or windowed aggregate in the
	 * given clause.
	 *
	 * @param aggFinder Finder for the particular kind(s) of aggregate function
	 * @param node      Parse tree
	 * @param clause    Name of clause: "WHERE", "GROUP BY", "ON"
	 */
	private void validateNoAggs(AggFinder aggFinder, SqlNode node,
		String clause) {
		final SqlCall agg = aggFinder.findAgg(node);
		if (agg == null) {
			return;
		}
		final SqlOperator op = agg.getOperator();
		if (op == SqlStdOperatorTable.OVER) {
			throw newValidationError(agg,
				RESOURCE.windowedAggregateIllegalInClause(clause));
		} else if (op.isGroup() || op.isGroupAuxiliary()) {
			throw newValidationError(agg,
				RESOURCE.groupFunctionMustAppearInGroupByClause(op.getName()));
		} else {
			throw newValidationError(agg,
				RESOURCE.aggregateIllegalInClause(clause));
		}
	}

	private RelDataType validateUsingCol(SqlIdentifier id, SqlNode leftOrRight) {
		if (id.names.size() == 1) {
			String name = id.names.get(0);
			final SqlValidatorNamespace namespace = getNamespace(leftOrRight);
			final RelDataType rowType = namespace.getRowType();
			final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
			final RelDataTypeField field = nameMatcher.field(rowType, name);
			if (field != null) {
				if (nameMatcher.frequency(rowType.getFieldNames(), name) > 1) {
					throw newValidationError(id,
						RESOURCE.columnInUsingNotUnique(id.toString()));
				}
				return field.getType();
			}
		}
		throw newValidationError(id, RESOURCE.columnNotFound(id.toString()));
	}

	/**
	 * Validates a SELECT statement.
	 *
	 * @param select        Select statement
	 * @param targetRowType Desired row type, must not be null, may be the data
	 *                      type 'unknown'.
	 */
	protected void validateSelect(
		SqlSelect select,
		RelDataType targetRowType) {
		assert targetRowType != null;
		// Namespace is either a select namespace or a wrapper around one.
		final SelectNamespace ns =
			getNamespace(select).unwrap(SelectNamespace.class);

		// Its rowtype is null, meaning it hasn't been validated yet.
		// This is important, because we need to take the targetRowType into
		// account.
		assert ns.rowType == null;

		if (select.isDistinct()) {
			validateFeature(RESOURCE.sQLFeature_E051_01(),
				select.getModifierNode(SqlSelectKeyword.DISTINCT)
					.getParserPosition());
		}

		final SqlNodeList selectItems = select.getSelectList();
		RelDataType fromType = unknownType;
		if (selectItems.size() == 1) {
			final SqlNode selectItem = selectItems.get(0);
			if (selectItem instanceof SqlIdentifier) {
				SqlIdentifier id = (SqlIdentifier) selectItem;
				if (id.isStar() && (id.names.size() == 1)) {
					// Special case: for INSERT ... VALUES(?,?), the SQL
					// standard says we're supposed to propagate the target
					// types down.  So iff the select list is an unqualified
					// star (as it will be after an INSERT ... VALUES has been
					// expanded), then propagate.
					fromType = targetRowType;
				}
			}
		}

		// Make sure that items in FROM clause have distinct aliases.
		final SelectScope fromScope = (SelectScope) getFromScope(select);
		List<String> names = fromScope.getChildNames();
		if (!catalogReader.nameMatcher().isCaseSensitive()) {
			names = Lists.transform(names, s -> s.toUpperCase(Locale.ROOT));
		}
		final int duplicateAliasOrdinal = Util.firstDuplicate(names);
		if (duplicateAliasOrdinal >= 0) {
			final ScopeChild child =
				fromScope.children.get(duplicateAliasOrdinal);
			throw newValidationError(child.namespace.getEnclosingNode(),
				RESOURCE.fromAliasDuplicate(child.name));
		}

		if (select.getFrom() == null) {
			if (conformance.isFromRequired()) {
				throw newValidationError(select, RESOURCE.selectMissingFrom());
			}
		} else {
			validateFrom(select.getFrom(), fromType, fromScope);
		}

		validateWhereClause(select);
		validateGroupClause(select);
		validateHavingClause(select);
		validateWindowClause(select);
		handleOffsetFetch(select.getOffset(), select.getFetch());

		// Validate the SELECT clause late, because a select item might
		// depend on the GROUP BY list, or the window function might reference
		// window name in the WINDOW clause etc.
		final RelDataType rowType =
			validateSelectList(selectItems, select, targetRowType);
		ns.setType(rowType);

		// Validate ORDER BY after we have set ns.rowType because in some
		// dialects you can refer to columns of the select list, e.g.
		// "SELECT empno AS x FROM emp ORDER BY x"
		validateOrderList(select);

		if (shouldCheckForRollUp(select.getFrom())) {
			checkRollUpInSelectList(select);
			checkRollUp(null, select, select.getWhere(), getWhereScope(select));
			checkRollUp(null, select, select.getHaving(), getHavingScope(select));
			checkRollUpInWindowDecl(select);
			checkRollUpInGroupBy(select);
			checkRollUpInOrderBy(select);
		}
	}

	private void checkRollUpInSelectList(SqlSelect select) {
		SqlValidatorScope scope = getSelectScope(select);
		for (SqlNode item : select.getSelectList()) {
			checkRollUp(null, select, item, scope);
		}
	}

	private void checkRollUpInGroupBy(SqlSelect select) {
		SqlNodeList group = select.getGroup();
		if (group != null) {
			for (SqlNode node : group) {
				checkRollUp(null, select, node, getGroupScope(select), "GROUP BY");
			}
		}
	}

	private void checkRollUpInOrderBy(SqlSelect select) {
		SqlNodeList orderList = select.getOrderList();
		if (orderList != null) {
			for (SqlNode node : orderList) {
				checkRollUp(null, select, node, getOrderScope(select), "ORDER BY");
			}
		}
	}

	private void checkRollUpInWindow(SqlWindow window, SqlValidatorScope scope) {
		if (window != null) {
			for (SqlNode node : window.getPartitionList()) {
				checkRollUp(null, window, node, scope, "PARTITION BY");
			}

			for (SqlNode node : window.getOrderList()) {
				checkRollUp(null, window, node, scope, "ORDER BY");
			}
		}
	}

	private void checkRollUpInWindowDecl(SqlSelect select) {
		for (SqlNode decl : select.getWindowList()) {
			checkRollUpInWindow((SqlWindow) decl, getSelectScope(select));
		}
	}

	private SqlNode stripDot(SqlNode node) {
		if (node != null && node.getKind() == SqlKind.DOT) {
			return stripDot(((SqlCall) node).operand(0));
		}
		return node;
	}

	private void checkRollUp(SqlNode grandParent, SqlNode parent,
		SqlNode current, SqlValidatorScope scope, String optionalClause) {
		current = stripAs(current);
		if (current instanceof SqlCall && !(current instanceof SqlSelect)) {
			// Validate OVER separately
			checkRollUpInWindow(getWindowInOver(current), scope);
			current = stripOver(current);

			List<SqlNode> children = ((SqlCall) stripDot(current)).getOperandList();
			for (SqlNode child : children) {
				checkRollUp(parent, current, child, scope, optionalClause);
			}
		} else if (current instanceof SqlIdentifier) {
			SqlIdentifier id = (SqlIdentifier) current;
			if (!id.isStar() && isRolledUpColumn(id, scope)) {
				if (!isAggregation(parent.getKind())
					|| !isRolledUpColumnAllowedInAgg(id, scope, (SqlCall) parent, grandParent)) {
					String context = optionalClause != null ? optionalClause : parent.getKind().toString();
					throw newValidationError(id,
						RESOURCE.rolledUpNotAllowed(deriveAlias(id, 0), context));
				}
			}
		}
	}

	private void checkRollUp(SqlNode grandParent, SqlNode parent,
		SqlNode current, SqlValidatorScope scope) {
		checkRollUp(grandParent, parent, current, scope, null);
	}

	private SqlWindow getWindowInOver(SqlNode over) {
		if (over.getKind() == SqlKind.OVER) {
			SqlNode window = ((SqlCall) over).getOperandList().get(1);
			if (window instanceof SqlWindow) {
				return (SqlWindow) window;
			}
			// SqlIdentifier, gets validated elsewhere
			return null;
		}
		return null;
	}

	private static SqlNode stripOver(SqlNode node) {
		switch (node.getKind()) {
			case OVER:
				return ((SqlCall) node).getOperandList().get(0);
			default:
				return node;
		}
	}

	private Pair<String, String> findTableColumnPair(SqlIdentifier identifier,
			SqlValidatorScope scope) {
		final SqlCall call = makeNullaryCall(identifier);
		if (call != null) {
			return null;
		}
		SqlQualified qualified = scope.fullyQualify(identifier);
		List<String> names = qualified.identifier.names;

		if (names.size() < 2) {
			return null;
		}

		return new Pair<>(names.get(names.size() - 2), Util.last(names));
	}

	// Returns true iff the given column is valid inside the given aggCall.
	private boolean isRolledUpColumnAllowedInAgg(SqlIdentifier identifier, SqlValidatorScope scope,
		SqlCall aggCall, SqlNode parent) {
		Pair<String, String> pair = findTableColumnPair(identifier, scope);

		if (pair == null) {
			return true;
		}

		String columnName = pair.right;

		SqlValidatorTable sqlValidatorTable =
				scope.fullyQualify(identifier).namespace.getTable();
		if (sqlValidatorTable != null) {
			Table table = sqlValidatorTable.unwrap(Table.class);
			return table.rolledUpColumnValidInsideAgg(columnName, aggCall, parent,
					catalogReader.getConfig());
		}
		return true;
	}


	// Returns true iff the given column is actually rolled up.
	private boolean isRolledUpColumn(SqlIdentifier identifier, SqlValidatorScope scope) {
		Pair<String, String> pair = findTableColumnPair(identifier, scope);

		if (pair == null) {
			return false;
		}

		String columnName = pair.right;

		SqlValidatorTable sqlValidatorTable =
				scope.fullyQualify(identifier).namespace.getTable();
		if (sqlValidatorTable != null) {
			Table table = sqlValidatorTable.unwrap(Table.class);
			return table.isRolledUp(columnName);
		}
		return false;
	}

	private boolean shouldCheckForRollUp(SqlNode from) {
		if (from != null) {
			SqlKind kind = stripAs(from).getKind();
			return kind != SqlKind.VALUES && kind != SqlKind.SELECT;
		}
		return false;
	}

	/** Validates that a query can deliver the modality it promises. Only called
	 * on the top-most SELECT or set operator in the tree. */
	private void validateModality(SqlNode query) {
		final SqlModality modality = deduceModality(query);
		if (query instanceof SqlSelect) {
			final SqlSelect select = (SqlSelect) query;
			validateModality(select, modality, true);
		} else if (query.getKind() == SqlKind.VALUES) {
			switch (modality) {
				case STREAM:
					throw newValidationError(query, Static.RESOURCE.cannotStreamValues());
			}
		} else {
			assert query.isA(SqlKind.SET_QUERY);
			final SqlCall call = (SqlCall) query;
			for (SqlNode operand : call.getOperandList()) {
				if (deduceModality(operand) != modality) {
					throw newValidationError(operand,
						Static.RESOURCE.streamSetOpInconsistentInputs());
				}
				validateModality(operand);
			}
		}
	}

	/** Return the intended modality of a SELECT or set-op. */
	private SqlModality deduceModality(SqlNode query) {
		if (query instanceof SqlSelect) {
			SqlSelect select = (SqlSelect) query;
			return select.getModifierNode(SqlSelectKeyword.STREAM) != null
				? SqlModality.STREAM
				: SqlModality.RELATION;
		} else if (query.getKind() == SqlKind.VALUES) {
			return SqlModality.RELATION;
		} else {
			assert query.isA(SqlKind.SET_QUERY);
			final SqlCall call = (SqlCall) query;
			return deduceModality(call.getOperandList().get(0));
		}
	}

	public boolean validateModality(SqlSelect select, SqlModality modality,
		boolean fail) {
		final SelectScope scope = getRawSelectScope(select);

		switch (modality) {
			case STREAM:
				if (scope.children.size() == 1) {
					for (ScopeChild child : scope.children) {
						if (!child.namespace.supportsModality(modality)) {
							if (fail) {
								throw newValidationError(child.namespace.getNode(),
									Static.RESOURCE.cannotConvertToStream(child.name));
							} else {
								return false;
							}
						}
					}
				} else {
					int supportsModalityCount = 0;
					for (ScopeChild child : scope.children) {
						if (child.namespace.supportsModality(modality)) {
							++supportsModalityCount;
						}
					}

					if (supportsModalityCount == 0) {
						if (fail) {
							String inputs = String.join(", ", scope.getChildNames());
							throw newValidationError(select,
								Static.RESOURCE.cannotStreamResultsForNonStreamingInputs(inputs));
						} else {
							return false;
						}
					}
				}
				break;
			default:
				for (ScopeChild child : scope.children) {
					if (!child.namespace.supportsModality(modality)) {
						if (fail) {
							throw newValidationError(child.namespace.getNode(),
								Static.RESOURCE.cannotConvertToRelation(child.name));
						} else {
							return false;
						}
					}
				}
		}

		// Make sure that aggregation is possible.
		final SqlNode aggregateNode = getAggregate(select);
		if (aggregateNode != null) {
			switch (modality) {
				case STREAM:
					SqlNodeList groupList = select.getGroup();
					if (groupList == null
						|| !SqlValidatorUtil.containsMonotonic(scope, groupList)) {
						if (fail) {
							throw newValidationError(aggregateNode,
								Static.RESOURCE.streamMustGroupByMonotonic());
						} else {
							return false;
						}
					}
			}
		}

		// Make sure that ORDER BY is possible.
		final SqlNodeList orderList  = select.getOrderList();
		if (orderList != null && orderList.size() > 0) {
			switch (modality) {
				case STREAM:
					if (!hasSortedPrefix(scope, orderList)) {
						if (fail) {
							throw newValidationError(orderList.get(0),
								Static.RESOURCE.streamMustOrderByMonotonic());
						} else {
							return false;
						}
					}
			}
		}
		return true;
	}

	/** Returns whether the prefix is sorted. */
	private boolean hasSortedPrefix(SelectScope scope, SqlNodeList orderList) {
		return isSortCompatible(scope, orderList.get(0), false);
	}

	private boolean isSortCompatible(SelectScope scope, SqlNode node,
		boolean descending) {
		switch (node.getKind()) {
			case DESCENDING:
				return isSortCompatible(scope, ((SqlCall) node).getOperandList().get(0),
					true);
		}
		final SqlMonotonicity monotonicity = scope.getMonotonicity(node);
		switch (monotonicity) {
			case INCREASING:
			case STRICTLY_INCREASING:
				return !descending;
			case DECREASING:
			case STRICTLY_DECREASING:
				return descending;
			default:
				return false;
		}
	}

	protected void validateWindowClause(SqlSelect select) {
		final SqlNodeList windowList = select.getWindowList();
		@SuppressWarnings("unchecked") final List<SqlWindow> windows =
			(List) windowList.getList();
		if (windows.isEmpty()) {
			return;
		}

		final SelectScope windowScope = (SelectScope) getFromScope(select);
		assert windowScope != null;

		// 1. ensure window names are simple
		// 2. ensure they are unique within this scope
		for (SqlWindow window : windows) {
			SqlIdentifier declName = window.getDeclName();
			if (!declName.isSimple()) {
				throw newValidationError(declName, RESOURCE.windowNameMustBeSimple());
			}

			if (windowScope.existingWindowName(declName.toString())) {
				throw newValidationError(declName, RESOURCE.duplicateWindowName());
			} else {
				windowScope.addWindowName(declName.toString());
			}
		}

		// 7.10 rule 2
		// Check for pairs of windows which are equivalent.
		for (int i = 0; i < windows.size(); i++) {
			SqlNode window1 = windows.get(i);
			for (int j = i + 1; j < windows.size(); j++) {
				SqlNode window2 = windows.get(j);
				if (window1.equalsDeep(window2, Litmus.IGNORE)) {
					throw newValidationError(window2, RESOURCE.dupWindowSpec());
				}
			}
		}

		for (SqlWindow window : windows) {
			final SqlNodeList expandedOrderList =
				(SqlNodeList) expand(window.getOrderList(), windowScope);
			window.setOrderList(expandedOrderList);
			expandedOrderList.validate(this, windowScope);

			final SqlNodeList expandedPartitionList =
				(SqlNodeList) expand(window.getPartitionList(), windowScope);
			window.setPartitionList(expandedPartitionList);
			expandedPartitionList.validate(this, windowScope);
		}

		// Hand off to validate window spec components
		windowList.validate(this, windowScope);
	}

	public void validateWith(SqlWith with, SqlValidatorScope scope) {
		final SqlValidatorNamespace namespace = getNamespace(with);
		validateNamespace(namespace, unknownType);
	}

	public void validateWithItem(SqlWithItem withItem) {
		if (withItem.columnList != null) {
			final RelDataType rowType = getValidatedNodeType(withItem.query);
			final int fieldCount = rowType.getFieldCount();
			if (withItem.columnList.size() != fieldCount) {
				throw newValidationError(withItem.columnList,
					RESOURCE.columnCountMismatch());
			}
			SqlValidatorUtil.checkIdentifierListForDuplicates(
				withItem.columnList.getList(), validationErrorFunction);
		} else {
			// Luckily, field names have not been make unique yet.
			final List<String> fieldNames =
				getValidatedNodeType(withItem.query).getFieldNames();
			final int i = Util.firstDuplicate(fieldNames);
			if (i >= 0) {
				throw newValidationError(withItem.query,
					RESOURCE.duplicateColumnAndNoColumnList(fieldNames.get(i)));
			}
		}
	}

	public void validateSequenceValue(SqlValidatorScope scope, SqlIdentifier id) {
		// Resolve identifier as a table.
		final SqlValidatorScope.ResolvedImpl resolved =
			new SqlValidatorScope.ResolvedImpl();
		scope.resolveTable(id.names, catalogReader.nameMatcher(),
			SqlValidatorScope.Path.EMPTY, resolved);
		if (resolved.count() != 1) {
			throw newValidationError(id, RESOURCE.tableNameNotFound(id.toString()));
		}
		// We've found a table. But is it a sequence?
		final SqlValidatorNamespace ns = resolved.only().namespace;
		if (ns instanceof TableNamespace) {
			final Table table = ns.getTable().unwrap(Table.class);
			switch (table.getJdbcTableType()) {
				case SEQUENCE:
				case TEMPORARY_SEQUENCE:
					return;
			}
		}
		throw newValidationError(id, RESOURCE.notASequence(id.toString()));
	}

	public SqlValidatorScope getWithScope(SqlNode withItem) {
		assert withItem.getKind() == SqlKind.WITH_ITEM;
		return scopes.get(withItem);
	}

	@Override
	public SqlValidator setEnableTypeCoercion(boolean enabled) {
		this.enableTypeCoercion = enabled;
		return this;
	}

	@Override
	public boolean isTypeCoercionEnabled() {
		return this.enableTypeCoercion;
	}

	@Override
	public void setTypeCoercion(TypeCoercion typeCoercion) {
		Objects.requireNonNull(typeCoercion);
		this.typeCoercion = typeCoercion;
	}

	@Override
	public TypeCoercion getTypeCoercion() {
		assert isTypeCoercionEnabled();
		return this.typeCoercion;
	}

	/**
	 * Validates the ORDER BY clause of a SELECT statement.
	 *
	 * @param select Select statement
	 */
	protected void validateOrderList(SqlSelect select) {
		// ORDER BY is validated in a scope where aliases in the SELECT clause
		// are visible. For example, "SELECT empno AS x FROM emp ORDER BY x"
		// is valid.
		SqlNodeList orderList = select.getOrderList();
		if (orderList == null) {
			return;
		}
		if (!shouldAllowIntermediateOrderBy()) {
			if (!cursorSet.contains(select)) {
				throw newValidationError(select, RESOURCE.invalidOrderByPos());
			}
		}
		final SqlValidatorScope orderScope = getOrderScope(select);
		Objects.requireNonNull(orderScope);

		List<SqlNode> expandList = new ArrayList<>();
		for (SqlNode orderItem : orderList) {
			SqlNode expandedOrderItem = expand(orderItem, orderScope);
			expandList.add(expandedOrderItem);
		}

		SqlNodeList expandedOrderList = new SqlNodeList(
			expandList,
			orderList.getParserPosition());
		select.setOrderBy(expandedOrderList);

		for (SqlNode orderItem : expandedOrderList) {
			validateOrderItem(select, orderItem);
		}
	}

	/**
	 * Validates an item in the GROUP BY clause of a SELECT statement.
	 *
	 * @param select Select statement
	 * @param groupByItem GROUP BY clause item
	 */
	private void validateGroupByItem(SqlSelect select, SqlNode groupByItem) {
		final SqlValidatorScope groupByScope = getGroupScope(select);
		groupByScope.validateExpr(groupByItem);
	}

	/**
	 * Validates an item in the ORDER BY clause of a SELECT statement.
	 *
	 * @param select Select statement
	 * @param orderItem ORDER BY clause item
	 */
	private void validateOrderItem(SqlSelect select, SqlNode orderItem) {
		switch (orderItem.getKind()) {
			case DESCENDING:
				validateFeature(RESOURCE.sQLConformance_OrderByDesc(),
					orderItem.getParserPosition());
				validateOrderItem(select,
					((SqlCall) orderItem).operand(0));
				return;
		}

		final SqlValidatorScope orderScope = getOrderScope(select);
		validateExpr(orderItem, orderScope);
	}

	public SqlNode expandOrderExpr(SqlSelect select, SqlNode orderExpr) {
		final SqlNode newSqlNode =
			new OrderExpressionExpander(select, orderExpr).go();
		if (newSqlNode != orderExpr) {
			final SqlValidatorScope scope = getOrderScope(select);
			inferUnknownTypes(unknownType, scope, newSqlNode);
			final RelDataType type = deriveType(scope, newSqlNode);
			setValidatedNodeType(newSqlNode, type);
		}
		return newSqlNode;
	}

	/**
	 * Validates the GROUP BY clause of a SELECT statement. This method is
	 * called even if no GROUP BY clause is present.
	 */
	protected void validateGroupClause(SqlSelect select) {
		SqlNodeList groupList = select.getGroup();
		if (groupList == null) {
			return;
		}
		final String clause = "GROUP BY";
		validateNoAggs(aggOrOverFinder, groupList, clause);
		final SqlValidatorScope groupScope = getGroupScope(select);
		inferUnknownTypes(unknownType, groupScope, groupList);

		// expand the expression in group list.
		List<SqlNode> expandedList = new ArrayList<>();
		for (SqlNode groupItem : groupList) {
			SqlNode expandedItem = expandGroupByOrHavingExpr(groupItem, groupScope, select, false);
			expandedList.add(expandedItem);
		}
		groupList = new SqlNodeList(expandedList, groupList.getParserPosition());
		select.setGroupBy(groupList);
		for (SqlNode groupItem : expandedList) {
			validateGroupByItem(select, groupItem);
		}

		// Nodes in the GROUP BY clause are expressions except if they are calls
		// to the GROUPING SETS, ROLLUP or CUBE operators; this operators are not
		// expressions, because they do not have a type.
		for (SqlNode node : groupList) {
			switch (node.getKind()) {
				case GROUPING_SETS:
				case ROLLUP:
				case CUBE:
					node.validate(this, groupScope);
					break;
				default:
					node.validateExpr(this, groupScope);
			}
		}

		// Derive the type of each GROUP BY item. We don't need the type, but
		// it resolves functions, and that is necessary for deducing
		// monotonicity.
		final SqlValidatorScope selectScope = getSelectScope(select);
		AggregatingSelectScope aggregatingScope = null;
		if (selectScope instanceof AggregatingSelectScope) {
			aggregatingScope = (AggregatingSelectScope) selectScope;
		}
		for (SqlNode groupItem : groupList) {
			if (groupItem instanceof SqlNodeList
				&& ((SqlNodeList) groupItem).size() == 0) {
				continue;
			}
			validateGroupItem(groupScope, aggregatingScope, groupItem);
		}

		SqlNode agg = aggFinder.findAgg(groupList);
		if (agg != null) {
			throw newValidationError(agg, RESOURCE.aggregateIllegalInClause(clause));
		}
	}

	private void validateGroupItem(SqlValidatorScope groupScope,
		AggregatingSelectScope aggregatingScope,
		SqlNode groupItem) {
		switch (groupItem.getKind()) {
			case GROUPING_SETS:
			case ROLLUP:
			case CUBE:
				validateGroupingSets(groupScope, aggregatingScope, (SqlCall) groupItem);
				break;
			default:
				if (groupItem instanceof SqlNodeList) {
					break;
				}
				final RelDataType type = deriveType(groupScope, groupItem);
				setValidatedNodeType(groupItem, type);
		}
	}

	private void validateGroupingSets(SqlValidatorScope groupScope,
		AggregatingSelectScope aggregatingScope, SqlCall groupItem) {
		for (SqlNode node : groupItem.getOperandList()) {
			validateGroupItem(groupScope, aggregatingScope, node);
		}
	}

	protected void validateWhereClause(SqlSelect select) {
		// validate WHERE clause
		final SqlNode where = select.getWhere();
		if (where == null) {
			return;
		}
		final SqlValidatorScope whereScope = getWhereScope(select);
		final SqlNode expandedWhere = expand(where, whereScope);
		select.setWhere(expandedWhere);
		validateWhereOrOn(whereScope, expandedWhere, "WHERE");
	}

	protected void validateWhereOrOn(
		SqlValidatorScope scope,
		SqlNode condition,
		String clause) {
		validateNoAggs(aggOrOverOrGroupFinder, condition, clause);
		inferUnknownTypes(
			booleanType,
			scope,
			condition);
		condition.validate(this, scope);

		final RelDataType type = deriveType(scope, condition);
		if (!SqlTypeUtil.inBooleanFamily(type)) {
			throw newValidationError(condition, RESOURCE.condMustBeBoolean(clause));
		}
	}

	protected void validateHavingClause(SqlSelect select) {
		// HAVING is validated in the scope after groups have been created.
		// For example, in "SELECT empno FROM emp WHERE empno = 10 GROUP BY
		// deptno HAVING empno = 10", the reference to 'empno' in the HAVING
		// clause is illegal.
		SqlNode having = select.getHaving();
		if (having == null) {
			return;
		}
		final AggregatingScope havingScope =
			(AggregatingScope) getSelectScope(select);
		if (getConformance().isHavingAlias()) {
			SqlNode newExpr = expandGroupByOrHavingExpr(having, havingScope, select, true);
			if (having != newExpr) {
				having = newExpr;
				select.setHaving(newExpr);
			}
		}
		havingScope.checkAggregateExpr(having, true);
		inferUnknownTypes(
			booleanType,
			havingScope,
			having);
		having.validate(this, havingScope);
		final RelDataType type = deriveType(havingScope, having);
		if (!SqlTypeUtil.inBooleanFamily(type)) {
			throw newValidationError(having, RESOURCE.havingMustBeBoolean());
		}
	}

	protected RelDataType validateSelectList(
		final SqlNodeList selectItems,
		SqlSelect select,
		RelDataType targetRowType) {
		// First pass, ensure that aliases are unique. "*" and "TABLE.*" items
		// are ignored.

		// Validate SELECT list. Expand terms of the form "*" or "TABLE.*".
		final SqlValidatorScope selectScope = getSelectScope(select);
		final List<SqlNode> expandedSelectItems = new ArrayList<>();
		final Set<String> aliases = new HashSet<>();
		final List<Map.Entry<String, RelDataType>> fieldList = new ArrayList<>();

		for (int i = 0; i < selectItems.size(); i++) {
			SqlNode selectItem = selectItems.get(i);
			if (selectItem instanceof SqlSelect) {
				handleScalarSubQuery(
					select,
					(SqlSelect) selectItem,
					expandedSelectItems,
					aliases,
					fieldList);
			} else {
				expandSelectItem(
					selectItem,
					select,
					targetRowType.isStruct()
						&& targetRowType.getFieldCount() >= i
						? targetRowType.getFieldList().get(i).getType()
						: unknownType,
					expandedSelectItems,
					aliases,
					fieldList,
					false);
			}
		}

		// Create the new select list with expanded items.  Pass through
		// the original parser position so that any overall failures can
		// still reference the original input text.
		SqlNodeList newSelectList =
			new SqlNodeList(
				expandedSelectItems,
				selectItems.getParserPosition());
		if (shouldExpandIdentifiers()) {
			select.setSelectList(newSelectList);
		}
		getRawSelectScope(select).setExpandedSelectList(expandedSelectItems);

		// TODO: when SELECT appears as a value sub-query, should be using
		// something other than unknownType for targetRowType
		inferUnknownTypes(targetRowType, selectScope, newSelectList);

		for (SqlNode selectItem : expandedSelectItems) {
			validateNoAggs(groupFinder, selectItem, "SELECT");
			validateExpr(selectItem, selectScope);
		}

		return typeFactory.createStructType(fieldList);
	}

	/**
	 * Validates an expression.
	 *
	 * @param expr  Expression
	 * @param scope Scope in which expression occurs
	 */
	private void validateExpr(SqlNode expr, SqlValidatorScope scope) {
		if (expr instanceof SqlCall) {
			final SqlOperator op = ((SqlCall) expr).getOperator();
			if (op.isAggregator() && op.requiresOver()) {
				throw newValidationError(expr,
					RESOURCE.absentOverClause());
			}
		}

		// Call on the expression to validate itself.
		expr.validateExpr(this, scope);

		// Perform any validation specific to the scope. For example, an
		// aggregating scope requires that expressions are valid aggregations.
		scope.validateExpr(expr);
	}

	/**
	 * Processes SubQuery found in Select list. Checks that is actually Scalar
	 * sub-query and makes proper entries in each of the 3 lists used to create
	 * the final rowType entry.
	 *
	 * @param parentSelect        base SqlSelect item
	 * @param selectItem          child SqlSelect from select list
	 * @param expandedSelectItems Select items after processing
	 * @param aliasList           built from user or system values
	 * @param fieldList           Built up entries for each select list entry
	 */
	private void handleScalarSubQuery(
		SqlSelect parentSelect,
		SqlSelect selectItem,
		List<SqlNode> expandedSelectItems,
		Set<String> aliasList,
		List<Map.Entry<String, RelDataType>> fieldList) {
		// A scalar sub-query only has one output column.
		if (1 != selectItem.getSelectList().size()) {
			throw newValidationError(selectItem,
				RESOURCE.onlyScalarSubQueryAllowed());
		}

		// No expansion in this routine just append to list.
		expandedSelectItems.add(selectItem);

		// Get or generate alias and add to list.
		final String alias =
			deriveAlias(
				selectItem,
				aliasList.size());
		aliasList.add(alias);

		final SelectScope scope = (SelectScope) getWhereScope(parentSelect);
		final RelDataType type = deriveType(scope, selectItem);
		setValidatedNodeType(selectItem, type);

		// we do not want to pass on the RelRecordType returned
		// by the sub query.  Just the type of the single expression
		// in the sub-query select list.
		assert type instanceof RelRecordType;
		RelRecordType rec = (RelRecordType) type;

		RelDataType nodeType = rec.getFieldList().get(0).getType();
		nodeType = typeFactory.createTypeWithNullability(nodeType, true);
		fieldList.add(Pair.of(alias, nodeType));
	}

	/**
	 * Derives a row-type for INSERT and UPDATE operations.
	 *
	 * @param table            Target table for INSERT/UPDATE
	 * @param targetColumnList List of target columns, or null if not specified
	 * @param append           Whether to append fields to those in <code>
	 *                         baseRowType</code>
	 * @return Rowtype
	 */
	protected RelDataType createTargetRowType(
		SqlValidatorTable table,
		SqlNodeList targetColumnList,
		boolean append) {
		RelDataType baseRowType = table.getRowType();
		if (targetColumnList == null) {
			return baseRowType;
		}
		List<RelDataTypeField> targetFields = baseRowType.getFieldList();
		final List<Map.Entry<String, RelDataType>> fields = new ArrayList<>();
		if (append) {
			for (RelDataTypeField targetField : targetFields) {
				fields.add(
					Pair.of(SqlUtil.deriveAliasFromOrdinal(fields.size()),
						targetField.getType()));
			}
		}
		final Set<Integer> assignedFields = new HashSet<>();
		final RelOptTable relOptTable = table instanceof RelOptTable
			? ((RelOptTable) table) : null;
		for (SqlNode node : targetColumnList) {
			SqlIdentifier id = (SqlIdentifier) node;
			RelDataTypeField targetField =
				SqlValidatorUtil.getTargetField(
					baseRowType, typeFactory, id, catalogReader, relOptTable);
			if (targetField == null) {
				throw newValidationError(id,
					RESOURCE.unknownTargetColumn(id.toString()));
			}
			if (!assignedFields.add(targetField.getIndex())) {
				throw newValidationError(id,
					RESOURCE.duplicateTargetColumn(targetField.getName()));
			}
			fields.add(targetField);
		}
		return typeFactory.createStructType(fields);
	}

	public void validateInsert(SqlInsert insert) {
		final SqlValidatorNamespace targetNamespace = getNamespace(insert);
		validateNamespace(targetNamespace, unknownType);
		final RelOptTable relOptTable = SqlValidatorUtil.getRelOptTable(
			targetNamespace, catalogReader.unwrap(Prepare.CatalogReader.class), null, null);
		final SqlValidatorTable table = relOptTable == null
			? targetNamespace.getTable()
			: relOptTable.unwrap(SqlValidatorTable.class);

		// INSERT has an optional column name list.  If present then
		// reduce the rowtype to the columns specified.  If not present
		// then the entire target rowtype is used.
		final RelDataType targetRowType =
			createTargetRowType(
				table,
				insert.getTargetColumnList(),
				false);

		final SqlNode source = insert.getSource();
		if (source instanceof SqlSelect) {
			final SqlSelect sqlSelect = (SqlSelect) source;
			validateSelect(sqlSelect, targetRowType);
		} else {
			final SqlValidatorScope scope = scopes.get(source);
			validateQuery(source, scope, targetRowType);
		}

		// REVIEW jvs 4-Dec-2008: In FRG-365, this namespace row type is
		// discarding the type inferred by inferUnknownTypes (which was invoked
		// from validateSelect above).  It would be better if that information
		// were used here so that we never saw any untyped nulls during
		// checkTypeAssignment.
		final RelDataType sourceRowType = getNamespace(source).getRowType();
		final RelDataType logicalTargetRowType =
			getLogicalTargetRowType(targetRowType, insert);
		setValidatedNodeType(insert, logicalTargetRowType);
		final RelDataType logicalSourceRowType =
			getLogicalSourceRowType(sourceRowType, insert);

		checkFieldCount(insert.getTargetTable(), table, source,
			logicalSourceRowType, logicalTargetRowType);

		checkTypeAssignment(logicalSourceRowType, logicalTargetRowType, insert);

		checkConstraint(table, source, logicalTargetRowType);

		validateAccess(insert.getTargetTable(), table, SqlAccessEnum.INSERT);
	}

	/**
	 * Validates insert values against the constraint of a modifiable view.
	 *
	 * @param validatorTable Table that may wrap a ModifiableViewTable
	 * @param source        The values being inserted
	 * @param targetRowType The target type for the view
	 */
	private void checkConstraint(
		SqlValidatorTable validatorTable,
		SqlNode source,
		RelDataType targetRowType) {
		final ModifiableViewTable modifiableViewTable =
			validatorTable.unwrap(ModifiableViewTable.class);
		if (modifiableViewTable != null && source instanceof SqlCall) {
			final Table table = modifiableViewTable.unwrap(Table.class);
			final RelDataType tableRowType = table.getRowType(typeFactory);
			final List<RelDataTypeField> tableFields = tableRowType.getFieldList();

			// Get the mapping from column indexes of the underlying table
			// to the target columns and view constraints.
			final Map<Integer, RelDataTypeField> tableIndexToTargetField =
				SqlValidatorUtil.getIndexToFieldMap(tableFields, targetRowType);
			final Map<Integer, RexNode> projectMap =
				RelOptUtil.getColumnConstraints(modifiableViewTable, targetRowType, typeFactory);

			// Determine columns (indexed to the underlying table) that need
			// to be validated against the view constraint.
			final ImmutableBitSet targetColumns =
				ImmutableBitSet.of(tableIndexToTargetField.keySet());
			final ImmutableBitSet constrainedColumns =
				ImmutableBitSet.of(projectMap.keySet());
			final ImmutableBitSet constrainedTargetColumns =
				targetColumns.intersect(constrainedColumns);

			// Validate insert values against the view constraint.
			final List<SqlNode> values = ((SqlCall) source).getOperandList();
			for (final int colIndex : constrainedTargetColumns.asList()) {
				final String colName = tableFields.get(colIndex).getName();
				final RelDataTypeField targetField = tableIndexToTargetField.get(colIndex);
				for (SqlNode row : values) {
					final SqlCall call = (SqlCall) row;
					final SqlNode sourceValue = call.operand(targetField.getIndex());
					final ValidationError validationError =
						new ValidationError(sourceValue,
							RESOURCE.viewConstraintNotSatisfied(colName,
								Util.last(validatorTable.getQualifiedName())));
					RelOptUtil.validateValueAgainstConstraint(sourceValue,
						projectMap.get(colIndex), validationError);
				}
			}
		}
	}

	/**
	 * Validates updates against the constraint of a modifiable view.
	 *
	 * @param validatorTable A {@link SqlValidatorTable} that may wrap a
	 *                       ModifiableViewTable
	 * @param update         The UPDATE parse tree node
	 * @param targetRowType  The target type
	 */
	private void checkConstraint(
		SqlValidatorTable validatorTable,
		SqlUpdate update,
		RelDataType targetRowType) {
		final ModifiableViewTable modifiableViewTable =
			validatorTable.unwrap(ModifiableViewTable.class);
		if (modifiableViewTable != null) {
			final Table table = modifiableViewTable.unwrap(Table.class);
			final RelDataType tableRowType = table.getRowType(typeFactory);

			final Map<Integer, RexNode> projectMap =
				RelOptUtil.getColumnConstraints(modifiableViewTable, targetRowType,
					typeFactory);
			final Map<String, Integer> nameToIndex =
				SqlValidatorUtil.mapNameToIndex(tableRowType.getFieldList());

			// Validate update values against the view constraint.
			final List<SqlNode> targets = update.getTargetColumnList().getList();
			final List<SqlNode> sources = update.getSourceExpressionList().getList();
			for (final Pair<SqlNode, SqlNode> column : Pair.zip(targets, sources)) {
				final String columnName = ((SqlIdentifier) column.left).getSimple();
				final Integer columnIndex = nameToIndex.get(columnName);
				if (projectMap.containsKey(columnIndex)) {
					final RexNode columnConstraint = projectMap.get(columnIndex);
					final ValidationError validationError =
						new ValidationError(column.right,
							RESOURCE.viewConstraintNotSatisfied(columnName,
								Util.last(validatorTable.getQualifiedName())));
					RelOptUtil.validateValueAgainstConstraint(column.right,
						columnConstraint, validationError);
				}
			}
		}
	}

	private void checkFieldCount(SqlNode node, SqlValidatorTable table,
		SqlNode source, RelDataType logicalSourceRowType,
		RelDataType logicalTargetRowType) {
		final int sourceFieldCount = logicalSourceRowType.getFieldCount();
		final int targetFieldCount = logicalTargetRowType.getFieldCount();
		if (sourceFieldCount != targetFieldCount) {
			throw newValidationError(node,
				RESOURCE.unmatchInsertColumn(targetFieldCount, sourceFieldCount));
		}
		// Ensure that non-nullable fields are targeted.
		final InitializerContext rexBuilder =
			new InitializerContext() {
				public RexBuilder getRexBuilder() {
					return new RexBuilder(typeFactory);
				}

				public RexNode convertExpression(SqlNode e) {
					throw new UnsupportedOperationException();
				}
			};
		final List<ColumnStrategy> strategies =
			table.unwrap(RelOptTable.class).getColumnStrategies();
		for (final RelDataTypeField field : table.getRowType().getFieldList()) {
			final RelDataTypeField targetField =
				logicalTargetRowType.getField(field.getName(), true, false);
			switch (strategies.get(field.getIndex())) {
				case NOT_NULLABLE:
					assert !field.getType().isNullable();
					if (targetField == null) {
						throw newValidationError(node,
							RESOURCE.columnNotNullable(field.getName()));
					}
					break;
				case NULLABLE:
					assert field.getType().isNullable();
					break;
				case VIRTUAL:
				case STORED:
					if (targetField != null
						&& !isValuesWithDefault(source, targetField.getIndex())) {
						throw newValidationError(node,
							RESOURCE.insertIntoAlwaysGenerated(field.getName()));
					}
			}
		}
	}

	/** Returns whether a query uses {@code DEFAULT} to populate a given
	 *  column. */
	private boolean isValuesWithDefault(SqlNode source, int column) {
		switch (source.getKind()) {
			case VALUES:
				for (SqlNode operand : ((SqlCall) source).getOperandList()) {
					if (!isRowWithDefault(operand, column)) {
						return false;
					}
				}
				return true;
		}
		return false;
	}

	private boolean isRowWithDefault(SqlNode operand, int column) {
		switch (operand.getKind()) {
			case ROW:
				final SqlCall row = (SqlCall) operand;
				return row.getOperandList().size() >= column
					&& row.getOperandList().get(column).getKind() == SqlKind.DEFAULT;
		}
		return false;
	}

	protected RelDataType getLogicalTargetRowType(
		RelDataType targetRowType,
		SqlInsert insert) {
		if (insert.getTargetColumnList() == null
			&& conformance.isInsertSubsetColumnsAllowed()) {
			// Target an implicit subset of columns.
			final SqlNode source = insert.getSource();
			final RelDataType sourceRowType = getNamespace(source).getRowType();
			final RelDataType logicalSourceRowType =
				getLogicalSourceRowType(sourceRowType, insert);
			final RelDataType implicitTargetRowType =
				typeFactory.createStructType(
					targetRowType.getFieldList()
						.subList(0, logicalSourceRowType.getFieldCount()));
			final SqlValidatorNamespace targetNamespace = getNamespace(insert);
			validateNamespace(targetNamespace, implicitTargetRowType);
			return implicitTargetRowType;
		} else {
			// Either the set of columns are explicitly targeted, or target the full
			// set of columns.
			return targetRowType;
		}
	}

	protected RelDataType getLogicalSourceRowType(
		RelDataType sourceRowType,
		SqlInsert insert) {
		return sourceRowType;
	}

	protected void checkTypeAssignment(
		RelDataType sourceRowType,
		RelDataType targetRowType,
		final SqlNode query) {
		// NOTE jvs 23-Feb-2006: subclasses may allow for extra targets
		// representing system-maintained columns, so stop after all sources
		// matched
		List<RelDataTypeField> sourceFields = sourceRowType.getFieldList();
		List<RelDataTypeField> targetFields = targetRowType.getFieldList();
		final int sourceCount = sourceFields.size();
		for (int i = 0; i < sourceCount; ++i) {
			RelDataType sourceType = sourceFields.get(i).getType();
			RelDataType targetType = targetFields.get(i).getType();
			if (!SqlTypeUtil.canAssignFrom(targetType, sourceType)) {
				// FRG-255:  account for UPDATE rewrite; there's
				// probably a better way to do this.
				int iAdjusted = i;
				if (query instanceof SqlUpdate) {
					int nUpdateColumns =
						((SqlUpdate) query).getTargetColumnList().size();
					assert sourceFields.size() >= nUpdateColumns;
					iAdjusted -= sourceFields.size() - nUpdateColumns;
				}
				SqlNode node = getNthExpr(query, iAdjusted, sourceCount);
				String targetTypeString;
				String sourceTypeString;
				if (SqlTypeUtil.areCharacterSetsMismatched(
					sourceType,
					targetType)) {
					sourceTypeString = sourceType.getFullTypeString();
					targetTypeString = targetType.getFullTypeString();
				} else {
					sourceTypeString = sourceType.toString();
					targetTypeString = targetType.toString();
				}
				throw newValidationError(node,
					RESOURCE.typeNotAssignable(
						targetFields.get(i).getName(), targetTypeString,
						sourceFields.get(i).getName(), sourceTypeString));
			}
		}
	}

	/**
	 * Locates the n'th expression in an INSERT or UPDATE query.
	 *
	 * @param query       Query
	 * @param ordinal     Ordinal of expression
	 * @param sourceCount Number of expressions
	 * @return Ordinal'th expression, never null
	 */
	private SqlNode getNthExpr(SqlNode query, int ordinal, int sourceCount) {
		if (query instanceof SqlInsert) {
			SqlInsert insert = (SqlInsert) query;
			if (insert.getTargetColumnList() != null) {
				return insert.getTargetColumnList().get(ordinal);
			} else {
				return getNthExpr(
					insert.getSource(),
					ordinal,
					sourceCount);
			}
		} else if (query instanceof SqlUpdate) {
			SqlUpdate update = (SqlUpdate) query;
			if (update.getTargetColumnList() != null) {
				return update.getTargetColumnList().get(ordinal);
			} else if (update.getSourceExpressionList() != null) {
				return update.getSourceExpressionList().get(ordinal);
			} else {
				return getNthExpr(
					update.getSourceSelect(),
					ordinal,
					sourceCount);
			}
		} else if (query instanceof SqlSelect) {
			SqlSelect select = (SqlSelect) query;
			if (select.getSelectList().size() == sourceCount) {
				return select.getSelectList().get(ordinal);
			} else {
				return query; // give up
			}
		} else {
			return query; // give up
		}
	}

	public void validateDelete(SqlDelete call) {
		final SqlSelect sqlSelect = call.getSourceSelect();
		validateSelect(sqlSelect, unknownType);

		final SqlValidatorNamespace targetNamespace = getNamespace(call);
		validateNamespace(targetNamespace, unknownType);
		final SqlValidatorTable table = targetNamespace.getTable();

		validateAccess(call.getTargetTable(), table, SqlAccessEnum.DELETE);
	}

	public void validateUpdate(SqlUpdate call) {
		final SqlValidatorNamespace targetNamespace = getNamespace(call);
		validateNamespace(targetNamespace, unknownType);
		final RelOptTable relOptTable = SqlValidatorUtil.getRelOptTable(
			targetNamespace, catalogReader.unwrap(Prepare.CatalogReader.class), null, null);
		final SqlValidatorTable table = relOptTable == null
			? targetNamespace.getTable()
			: relOptTable.unwrap(SqlValidatorTable.class);

		final RelDataType targetRowType =
			createTargetRowType(
				table,
				call.getTargetColumnList(),
				true);

		final SqlSelect select = call.getSourceSelect();
		validateSelect(select, targetRowType);

		final RelDataType sourceRowType = getNamespace(call).getRowType();
		checkTypeAssignment(sourceRowType, targetRowType, call);

		checkConstraint(table, call, targetRowType);

		validateAccess(call.getTargetTable(), table, SqlAccessEnum.UPDATE);
	}

	public void validateMerge(SqlMerge call) {
		SqlSelect sqlSelect = call.getSourceSelect();
		// REVIEW zfong 5/25/06 - Does an actual type have to be passed into
		// validateSelect()?

		// REVIEW jvs 6-June-2006:  In general, passing unknownType like
		// this means we won't be able to correctly infer the types
		// for dynamic parameter markers (SET x = ?).  But
		// maybe validateUpdate and validateInsert below will do
		// the job?

		// REVIEW ksecretan 15-July-2011: They didn't get a chance to
		// since validateSelect() would bail.
		// Let's use the update/insert targetRowType when available.
		IdentifierNamespace targetNamespace =
			(IdentifierNamespace) getNamespace(call.getTargetTable());
		validateNamespace(targetNamespace, unknownType);

		SqlValidatorTable table = targetNamespace.getTable();
		validateAccess(call.getTargetTable(), table, SqlAccessEnum.UPDATE);

		RelDataType targetRowType = unknownType;

		if (call.getUpdateCall() != null) {
			targetRowType = createTargetRowType(
				table,
				call.getUpdateCall().getTargetColumnList(),
				true);
		}
		if (call.getInsertCall() != null) {
			targetRowType = createTargetRowType(
				table,
				call.getInsertCall().getTargetColumnList(),
				false);
		}

		validateSelect(sqlSelect, targetRowType);

		if (call.getUpdateCall() != null) {
			validateUpdate(call.getUpdateCall());
		}
		if (call.getInsertCall() != null) {
			validateInsert(call.getInsertCall());
		}
	}

	/**
	 * Validates access to a table.
	 *
	 * @param table          Table
	 * @param requiredAccess Access requested on table
	 */
	private void validateAccess(
		SqlNode node,
		SqlValidatorTable table,
		SqlAccessEnum requiredAccess) {
		if (table != null) {
			SqlAccessType access = table.getAllowedAccess();
			if (!access.allowsAccess(requiredAccess)) {
				throw newValidationError(node,
					RESOURCE.accessNotAllowed(requiredAccess.name(),
						table.getQualifiedName().toString()));
			}
		}
	}

	/**
	 * Validates a VALUES clause.
	 *
	 * @param node          Values clause
	 * @param targetRowType Row type which expression must conform to
	 * @param scope         Scope within which clause occurs
	 */
	protected void validateValues(
		SqlCall node,
		RelDataType targetRowType,
		final SqlValidatorScope scope) {
		assert node.getKind() == SqlKind.VALUES;

		final List<SqlNode> operands = node.getOperandList();
		for (SqlNode operand : operands) {
			if (!(operand.getKind() == SqlKind.ROW)) {
				throw Util.needToImplement(
					"Values function where operands are scalars");
			}

			SqlCall rowConstructor = (SqlCall) operand;
			if (conformance.isInsertSubsetColumnsAllowed() && targetRowType.isStruct()
				&& rowConstructor.operandCount() < targetRowType.getFieldCount()) {
				targetRowType =
					typeFactory.createStructType(
						targetRowType.getFieldList()
							.subList(0, rowConstructor.operandCount()));
			} else if (targetRowType.isStruct()
				&& rowConstructor.operandCount() != targetRowType.getFieldCount()) {
				return;
			}

			inferUnknownTypes(
				targetRowType,
				scope,
				rowConstructor);

			if (targetRowType.isStruct()) {
				for (Pair<SqlNode, RelDataTypeField> pair
					: Pair.zip(rowConstructor.getOperandList(),
					targetRowType.getFieldList())) {
					if (!pair.right.getType().isNullable()
						&& SqlUtil.isNullLiteral(pair.left, false)) {
						throw newValidationError(node,
							RESOURCE.columnNotNullable(pair.right.getName()));
					}
				}
			}
		}

		for (SqlNode operand : operands) {
			operand.validate(this, scope);
		}

		// validate that all row types have the same number of columns
		//  and that expressions in each column are compatible.
		// A values expression is turned into something that looks like
		// ROW(type00, type01,...), ROW(type11,...),...
		final int rowCount = operands.size();
		if (rowCount >= 2) {
			SqlCall firstRow = (SqlCall) operands.get(0);
			final int columnCount = firstRow.operandCount();

			// 1. check that all rows have the same cols length
			for (SqlNode operand : operands) {
				SqlCall thisRow = (SqlCall) operand;
				if (columnCount != thisRow.operandCount()) {
					throw newValidationError(node,
						RESOURCE.incompatibleValueType(
							SqlStdOperatorTable.VALUES.getName()));
				}
			}

			// 2. check if types at i:th position in each row are compatible
			for (int col = 0; col < columnCount; col++) {
				final int c = col;
				final RelDataType type =
					typeFactory.leastRestrictive(
						new AbstractList<RelDataType>() {
							public RelDataType get(int row) {
								SqlCall thisRow = (SqlCall) operands.get(row);
								return deriveType(scope, thisRow.operand(c));
							}

							public int size() {
								return rowCount;
							}
						});

				if (null == type) {
					throw newValidationError(node,
						RESOURCE.incompatibleValueType(
							SqlStdOperatorTable.VALUES.getName()));
				}
			}
		}
	}

	public void validateDataType(SqlDataTypeSpec dataType) {
	}

	public void validateDynamicParam(SqlDynamicParam dynamicParam) {
	}

	/**
	 * Throws a validator exception with access to the validator context.
	 * The exception is determined when an instance is created.
	 */
	private class ValidationError implements Supplier<CalciteContextException> {
		private final SqlNode sqlNode;
		private final Resources.ExInst<SqlValidatorException> validatorException;

		ValidationError(SqlNode sqlNode,
			Resources.ExInst<SqlValidatorException> validatorException) {
			this.sqlNode = sqlNode;
			this.validatorException = validatorException;
		}

		public CalciteContextException get() {
			return newValidationError(sqlNode, validatorException);
		}
	}

	/**
	 * Throws a validator exception with access to the validator context.
	 * The exception is determined when the function is applied.
	 */
	class ValidationErrorFunction
		implements Function2<SqlNode, Resources.ExInst<SqlValidatorException>,
		CalciteContextException> {
		@Override public CalciteContextException apply(
			SqlNode v0, Resources.ExInst<SqlValidatorException> v1) {
			return newValidationError(v0, v1);
		}
	}

	public ValidationErrorFunction getValidationErrorFunction() {
		return validationErrorFunction;
	}

	public CalciteContextException newValidationError(SqlNode node,
		Resources.ExInst<SqlValidatorException> e) {
		assert node != null;
		final SqlParserPos pos = node.getParserPosition();
		return SqlUtil.newContextException(pos, e);
	}

	protected SqlWindow getWindowByName(
		SqlIdentifier id,
		SqlValidatorScope scope) {
		SqlWindow window = null;
		if (id.isSimple()) {
			final String name = id.getSimple();
			window = scope.lookupWindow(name);
		}
		if (window == null) {
			throw newValidationError(id, RESOURCE.windowNotFound(id.toString()));
		}
		return window;
	}

	public SqlWindow resolveWindow(
		SqlNode windowOrRef,
		SqlValidatorScope scope,
		boolean populateBounds) {
		SqlWindow window;
		if (windowOrRef instanceof SqlIdentifier) {
			window = getWindowByName((SqlIdentifier) windowOrRef, scope);
		} else {
			window = (SqlWindow) windowOrRef;
		}
		while (true) {
			final SqlIdentifier refId = window.getRefName();
			if (refId == null) {
				break;
			}
			final String refName = refId.getSimple();
			SqlWindow refWindow = scope.lookupWindow(refName);
			if (refWindow == null) {
				throw newValidationError(refId, RESOURCE.windowNotFound(refName));
			}
			window = window.overlay(refWindow, this);
		}

		if (populateBounds) {
			window.populateBounds();
		}
		return window;
	}

	public SqlNode getOriginal(SqlNode expr) {
		SqlNode original = originalExprs.get(expr);
		if (original == null) {
			original = expr;
		}
		return original;
	}

	public void setOriginal(SqlNode expr, SqlNode original) {
		// Don't overwrite the original original.
		originalExprs.putIfAbsent(expr, original);
	}

	SqlValidatorNamespace lookupFieldNamespace(RelDataType rowType, String name) {
		final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
		final RelDataTypeField field = nameMatcher.field(rowType, name);
		if (field == null) {
			return null;
		}
		return new FieldNamespace(this, field.getType());
	}

	public void validateWindow(
		SqlNode windowOrId,
		SqlValidatorScope scope,
		SqlCall call) {
		// Enable nested aggregates with window aggregates (OVER operator)
		inWindow = true;

		final SqlWindow targetWindow;
		switch (windowOrId.getKind()) {
			case IDENTIFIER:
				// Just verify the window exists in this query.  It will validate
				// when the definition is processed
				targetWindow = getWindowByName((SqlIdentifier) windowOrId, scope);
				break;
			case WINDOW:
				targetWindow = (SqlWindow) windowOrId;
				break;
			default:
				throw Util.unexpected(windowOrId.getKind());
		}

		assert targetWindow.getWindowCall() == null;
		targetWindow.setWindowCall(call);
		targetWindow.validate(this, scope);
		targetWindow.setWindowCall(null);
		call.validate(this, scope);

		validateAggregateParams(call, null, null, scope);

		// Disable nested aggregates post validation
		inWindow = false;
	}

	@Override public void validateMatchRecognize(SqlCall call) {
		final SqlMatchRecognize matchRecognize = (SqlMatchRecognize) call;
		final MatchRecognizeScope scope =
			(MatchRecognizeScope) getMatchRecognizeScope(matchRecognize);

		final MatchRecognizeNamespace ns =
			getNamespace(call).unwrap(MatchRecognizeNamespace.class);
		assert ns.rowType == null;

		// rows per match
		final SqlLiteral rowsPerMatch = matchRecognize.getRowsPerMatch();
		final boolean allRows = rowsPerMatch != null
			&& rowsPerMatch.getValue()
			== SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS;

		final RelDataTypeFactory.Builder typeBuilder = typeFactory.builder();

		// parse PARTITION BY column
		SqlNodeList partitionBy = matchRecognize.getPartitionList();
		if (partitionBy != null) {
			for (SqlNode node : partitionBy) {
				SqlIdentifier identifier = (SqlIdentifier) node;
				identifier.validate(this, scope);
				RelDataType type = deriveType(scope, identifier);
				String name = identifier.names.get(1);
				typeBuilder.add(name, type);
			}
		}

		// parse ORDER BY column
		SqlNodeList orderBy = matchRecognize.getOrderList();
		if (orderBy != null) {
			for (SqlNode node : orderBy) {
				node.validate(this, scope);
				SqlIdentifier identifier;
				if (node instanceof SqlBasicCall) {
					identifier = (SqlIdentifier) ((SqlBasicCall) node).getOperands()[0];
				} else {
					identifier = (SqlIdentifier) node;
				}

				if (allRows) {
					RelDataType type = deriveType(scope, identifier);
					String name = identifier.names.get(1);
					if (!typeBuilder.nameExists(name)) {
						typeBuilder.add(name, type);
					}
				}
			}
		}

		if (allRows) {
			final SqlValidatorNamespace sqlNs =
				getNamespace(matchRecognize.getTableRef());
			final RelDataType inputDataType = sqlNs.getRowType();
			for (RelDataTypeField fs : inputDataType.getFieldList()) {
				if (!typeBuilder.nameExists(fs.getName())) {
					typeBuilder.add(fs);
				}
			}
		}

		// retrieve pattern variables used in pattern and subset
		SqlNode pattern = matchRecognize.getPattern();
		PatternVarVisitor visitor = new PatternVarVisitor(scope);
		pattern.accept(visitor);

		SqlLiteral interval = matchRecognize.getInterval();
		if (interval != null) {
			interval.validate(this, scope);
			if (((SqlIntervalLiteral) interval).signum() < 0) {
				throw newValidationError(interval,
					RESOURCE.intervalMustBeNonNegative(interval.toValue()));
			}
			if (orderBy == null || orderBy.size() == 0) {
				throw newValidationError(interval,
					RESOURCE.cannotUseWithinWithoutOrderBy());
			}

			SqlNode firstOrderByColumn = orderBy.getList().get(0);
			SqlIdentifier identifier;
			if (firstOrderByColumn instanceof SqlBasicCall) {
				identifier = (SqlIdentifier) ((SqlBasicCall) firstOrderByColumn).getOperands()[0];
			} else {
				identifier = (SqlIdentifier) firstOrderByColumn;
			}
			RelDataType firstOrderByColumnType = deriveType(scope, identifier);
			if (firstOrderByColumnType.getSqlTypeName() != SqlTypeName.TIMESTAMP) {
				throw newValidationError(interval,
					RESOURCE.firstColumnOfOrderByMustBeTimestamp());
			}

			SqlNode expand = expand(interval, scope);
			RelDataType type = deriveType(scope, expand);
			setValidatedNodeType(interval, type);
		}

		validateDefinitions(matchRecognize, scope);

		SqlNodeList subsets = matchRecognize.getSubsetList();
		if (subsets != null && subsets.size() > 0) {
			for (SqlNode node : subsets) {
				List<SqlNode> operands = ((SqlCall) node).getOperandList();
				String leftString = ((SqlIdentifier) operands.get(0)).getSimple();
				if (scope.getPatternVars().contains(leftString)) {
					throw newValidationError(operands.get(0),
						RESOURCE.patternVarAlreadyDefined(leftString));
				}
				scope.addPatternVar(leftString);
				for (SqlNode right : (SqlNodeList) operands.get(1)) {
					SqlIdentifier id = (SqlIdentifier) right;
					if (!scope.getPatternVars().contains(id.getSimple())) {
						throw newValidationError(id,
							RESOURCE.unknownPattern(id.getSimple()));
					}
					scope.addPatternVar(id.getSimple());
				}
			}
		}

		// validate AFTER ... SKIP TO
		final SqlNode skipTo = matchRecognize.getAfter();
		if (skipTo instanceof SqlCall) {
			final SqlCall skipToCall = (SqlCall) skipTo;
			final SqlIdentifier id = skipToCall.operand(0);
			if (!scope.getPatternVars().contains(id.getSimple())) {
				throw newValidationError(id,
					RESOURCE.unknownPattern(id.getSimple()));
			}
		}

		List<Map.Entry<String, RelDataType>> measureColumns =
			validateMeasure(matchRecognize, scope, allRows);
		for (Map.Entry<String, RelDataType> c : measureColumns) {
			if (!typeBuilder.nameExists(c.getKey())) {
				typeBuilder.add(c.getKey(), c.getValue());
			}
		}

		final RelDataType rowType = typeBuilder.build();
		if (matchRecognize.getMeasureList().size() == 0) {
			ns.setType(getNamespace(matchRecognize.getTableRef()).getRowType());
		} else {
			ns.setType(rowType);
		}
	}

	private List<Map.Entry<String, RelDataType>> validateMeasure(SqlMatchRecognize mr,
		MatchRecognizeScope scope, boolean allRows) {
		final List<String> aliases = new ArrayList<>();
		final List<SqlNode> sqlNodes = new ArrayList<>();
		final SqlNodeList measures = mr.getMeasureList();
		final List<Map.Entry<String, RelDataType>> fields = new ArrayList<>();

		for (SqlNode measure : measures) {
			assert measure instanceof SqlCall;
			final String alias = deriveAlias(measure, aliases.size());
			aliases.add(alias);

			SqlNode expand = expand(measure, scope);
			expand = navigationInMeasure(expand, allRows);
			setOriginal(expand, measure);

			inferUnknownTypes(unknownType, scope, expand);
			final RelDataType type = deriveType(scope, expand);
			setValidatedNodeType(measure, type);

			fields.add(Pair.of(alias, type));
			sqlNodes.add(
				SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, expand,
					new SqlIdentifier(alias, SqlParserPos.ZERO)));
		}

		SqlNodeList list = new SqlNodeList(sqlNodes, measures.getParserPosition());
		inferUnknownTypes(unknownType, scope, list);

		for (SqlNode node : list) {
			validateExpr(node, scope);
		}

		mr.setOperand(SqlMatchRecognize.OPERAND_MEASURES, list);

		return fields;
	}

	private SqlNode navigationInMeasure(SqlNode node, boolean allRows) {
		final Set<String> prefix = node.accept(new PatternValidator(true));
		Util.discard(prefix);
		final List<SqlNode> ops = ((SqlCall) node).getOperandList();

		final SqlOperator defaultOp =
			allRows ? SqlStdOperatorTable.RUNNING : SqlStdOperatorTable.FINAL;
		final SqlNode op0 = ops.get(0);
		if (!isRunningOrFinal(op0.getKind())
			|| !allRows && op0.getKind() == SqlKind.RUNNING) {
			SqlNode newNode = defaultOp.createCall(SqlParserPos.ZERO, op0);
			node = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, newNode, ops.get(1));
		}

		node = new NavigationExpander().go(node);
		return node;
	}

	private void validateDefinitions(SqlMatchRecognize mr,
		MatchRecognizeScope scope) {
		final Set<String> aliases = catalogReader.nameMatcher().createSet();
		for (SqlNode item : mr.getPatternDefList().getList()) {
			final String alias = alias(item);
			if (!aliases.add(alias)) {
				throw newValidationError(item,
					Static.RESOURCE.patternVarAlreadyDefined(alias));
			}
			scope.addPatternVar(alias);
		}

		final List<SqlNode> sqlNodes = new ArrayList<>();
		for (SqlNode item : mr.getPatternDefList().getList()) {
			final String alias = alias(item);
			SqlNode expand = expand(item, scope);
			expand = navigationInDefine(expand, alias);
			setOriginal(expand, item);

			inferUnknownTypes(booleanType, scope, expand);
			expand.validate(this, scope);

			// Some extra work need required here.
			// In PREV, NEXT, FINAL and LAST, only one pattern variable is allowed.
			sqlNodes.add(
				SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, expand,
					new SqlIdentifier(alias, SqlParserPos.ZERO)));

			final RelDataType type = deriveType(scope, expand);
			if (!SqlTypeUtil.inBooleanFamily(type)) {
				throw newValidationError(expand, RESOURCE.condMustBeBoolean("DEFINE"));
			}
			setValidatedNodeType(item, type);
		}

		SqlNodeList list =
			new SqlNodeList(sqlNodes, mr.getPatternDefList().getParserPosition());
		inferUnknownTypes(unknownType, scope, list);
		for (SqlNode node : list) {
			validateExpr(node, scope);
		}
		mr.setOperand(SqlMatchRecognize.OPERAND_PATTERN_DEFINES, list);
	}

	/** Returns the alias of a "expr AS alias" expression. */
	private static String alias(SqlNode item) {
		assert item instanceof SqlCall;
		assert item.getKind() == SqlKind.AS;
		final SqlIdentifier identifier = ((SqlCall) item).operand(1);
		return identifier.getSimple();
	}

	/** Checks that all pattern variables within a function are the same,
	 * and canonizes expressions such as {@code PREV(B.price)} to
	 * {@code LAST(B.price, 0)}. */
	private SqlNode navigationInDefine(SqlNode node, String alpha) {
		Set<String> prefix = node.accept(new PatternValidator(false));
		Util.discard(prefix);
		node = new NavigationExpander().go(node);
		node = new NavigationReplacer(alpha).go(node);
		return node;
	}

	public void validateAggregateParams(SqlCall aggCall, SqlNode filter,
			SqlNodeList orderList, SqlValidatorScope scope) {
		// For "agg(expr)", expr cannot itself contain aggregate function
		// invocations.  For example, "SUM(2 * MAX(x))" is illegal; when
		// we see it, we'll report the error for the SUM (not the MAX).
		// For more than one level of nesting, the error which results
		// depends on the traversal order for validation.
		//
		// For a windowed aggregate "agg(expr)", expr can contain an aggregate
		// function. For example,
		//   SELECT AVG(2 * MAX(x)) OVER (PARTITION BY y)
		//   FROM t
		//   GROUP BY y
		// is legal. Only one level of nesting is allowed since non-windowed
		// aggregates cannot nest aggregates.

		// Store nesting level of each aggregate. If an aggregate is found at an invalid
		// nesting level, throw an assert.
		final AggFinder a;
		if (inWindow) {
			a = overFinder;
		} else {
			a = aggOrOverFinder;
		}

		for (SqlNode param : aggCall.getOperandList()) {
			if (a.findAgg(param) != null) {
				throw newValidationError(aggCall, RESOURCE.nestedAggIllegal());
			}
		}
		if (filter != null) {
			if (a.findAgg(filter) != null) {
				throw newValidationError(filter, RESOURCE.aggregateInFilterIllegal());
			}
		}
		if (orderList != null) {
			for (SqlNode param : orderList) {
				if (a.findAgg(param) != null) {
					throw newValidationError(aggCall,
						RESOURCE.aggregateInWithinGroupIllegal());
				}
			}
		}

		final SqlAggFunction op = (SqlAggFunction) aggCall.getOperator();
		switch (op.requiresGroupOrder()) {
			case MANDATORY:
				if (orderList == null || orderList.size() == 0) {
					throw newValidationError(aggCall,
						RESOURCE.aggregateMissingWithinGroupClause(op.getName()));
				}
				break;
			case OPTIONAL:
				break;
			case IGNORED:
				// rewrite the order list to empty
				if (orderList != null) {
					orderList.getList().clear();
				}
				break;
			case FORBIDDEN:
				if (orderList != null && orderList.size() != 0) {
					throw newValidationError(aggCall,
						RESOURCE.withinGroupClauseIllegalInAggregate(op.getName()));
				}
				break;
			default:
				throw new AssertionError(op);
		}
	}

	public void validateCall(
		SqlCall call,
		SqlValidatorScope scope) {
		final SqlOperator operator = call.getOperator();
		if ((call.operandCount() == 0)
			&& (operator.getSyntax() == SqlSyntax.FUNCTION_ID)
			&& !call.isExpanded()
			&& !conformance.allowNiladicParentheses()) {
			// For example, "LOCALTIME()" is illegal. (It should be
			// "LOCALTIME", which would have been handled as a
			// SqlIdentifier.)
			throw handleUnresolvedFunction(call, (SqlFunction) operator,
				ImmutableList.of(), null);
		}

		SqlValidatorScope operandScope = scope.getOperandScope(call);

		if (operator instanceof SqlFunction
			&& ((SqlFunction) operator).getFunctionType()
			== SqlFunctionCategory.MATCH_RECOGNIZE
			&& !(operandScope instanceof MatchRecognizeScope)) {
			throw newValidationError(call,
				Static.RESOURCE.functionMatchRecognizeOnly(call.toString()));
		}
		// Delegate validation to the operator.
		operator.validateCall(call, this, scope, operandScope);
	}

	/**
	 * Validates that a particular feature is enabled. By default, all features
	 * are enabled; subclasses may override this method to be more
	 * discriminating.
	 *
	 * @param feature feature being used, represented as a resource instance
	 * @param context parser position context for error reporting, or null if
	 */
	protected void validateFeature(
		Feature feature,
		SqlParserPos context) {
		// By default, do nothing except to verify that the resource
		// represents a real feature definition.
		assert feature.getProperties().get("FeatureDefinition") != null;
	}

	public SqlNode expand(SqlNode expr, SqlValidatorScope scope) {
		final Expander expander = new Expander(this, scope);
		SqlNode newExpr = expr.accept(expander);
		if (expr != newExpr) {
			setOriginal(newExpr, expr);
		}
		return newExpr;
	}

	public SqlNode expandGroupByOrHavingExpr(SqlNode expr,
		SqlValidatorScope scope, SqlSelect select, boolean havingExpression) {
		final Expander expander = new ExtendedExpander(this, scope, select, expr,
			havingExpression);
		SqlNode newExpr = expr.accept(expander);
		if (expr != newExpr) {
			setOriginal(newExpr, expr);
		}
		return newExpr;
	}

	public boolean isSystemField(RelDataTypeField field) {
		return false;
	}

	public List<List<String>> getFieldOrigins(SqlNode sqlQuery) {
		if (sqlQuery instanceof SqlExplain) {
			return Collections.emptyList();
		}
		final RelDataType rowType = getValidatedNodeType(sqlQuery);
		final int fieldCount = rowType.getFieldCount();
		if (!sqlQuery.isA(SqlKind.QUERY)) {
			return Collections.nCopies(fieldCount, null);
		}
		final List<List<String>> list = new ArrayList<>();
		for (int i = 0; i < fieldCount; i++) {
			list.add(getFieldOrigin(sqlQuery, i));
		}
		return ImmutableNullableList.copyOf(list);
	}

	private List<String> getFieldOrigin(SqlNode sqlQuery, int i) {
		if (sqlQuery instanceof SqlSelect) {
			SqlSelect sqlSelect = (SqlSelect) sqlQuery;
			final SelectScope scope = getRawSelectScope(sqlSelect);
			final List<SqlNode> selectList = scope.getExpandedSelectList();
			final SqlNode selectItem = stripAs(selectList.get(i));
			if (selectItem instanceof SqlIdentifier) {
				final SqlQualified qualified =
					scope.fullyQualify((SqlIdentifier) selectItem);
				SqlValidatorNamespace namespace = qualified.namespace;
				final SqlValidatorTable table = namespace.getTable();
				if (table == null) {
					return null;
				}
				final List<String> origin =
					new ArrayList<>(table.getQualifiedName());
				for (String name : qualified.suffix()) {
					namespace = namespace.lookupChild(name);
					if (namespace == null) {
						return null;
					}
					origin.add(name);
				}
				return origin;
			}
			return null;
		} else if (sqlQuery instanceof SqlOrderBy) {
			return getFieldOrigin(((SqlOrderBy) sqlQuery).query, i);
		} else {
			return null;
		}
	}

	public RelDataType getParameterRowType(SqlNode sqlQuery) {
		// NOTE: We assume that bind variables occur in depth-first tree
		// traversal in the same order that they occurred in the SQL text.
		final List<RelDataType> types = new ArrayList<>();
		// NOTE: but parameters on fetch/offset would be counted twice
		// as they are counted in the SqlOrderBy call and the inner SqlSelect call
		final Set<SqlNode> alreadyVisited = new HashSet<>();
		sqlQuery.accept(
			new SqlShuttle() {

				@Override public SqlNode visit(SqlDynamicParam param) {
					if (alreadyVisited.add(param)) {
						RelDataType type = getValidatedNodeType(param);
						types.add(type);
					}
					return param;
				}
			});
		return typeFactory.createStructType(
			types,
			new AbstractList<String>() {
				@Override public String get(int index) {
					return "?" + index;
				}

				@Override public int size() {
					return types.size();
				}
			});
	}

	public void validateColumnListParams(
		SqlFunction function,
		List<RelDataType> argTypes,
		List<SqlNode> operands) {
		throw new UnsupportedOperationException();
	}

	private static boolean isPhysicalNavigation(SqlKind kind) {
		return kind == SqlKind.PREV || kind == SqlKind.NEXT;
	}

	private static boolean isLogicalNavigation(SqlKind kind) {
		return kind == SqlKind.FIRST || kind == SqlKind.LAST;
	}

	private static boolean isAggregation(SqlKind kind) {
		return kind == SqlKind.SUM || kind == SqlKind.SUM0
			|| kind == SqlKind.AVG || kind == SqlKind.COUNT
			|| kind == SqlKind.MAX || kind == SqlKind.MIN;
	}

	private static boolean isRunningOrFinal(SqlKind kind) {
		return kind == SqlKind.RUNNING || kind == SqlKind.FINAL;
	}

	private static boolean isSingleVarRequired(SqlKind kind) {
		return isPhysicalNavigation(kind)
			|| isLogicalNavigation(kind)
			|| isAggregation(kind);
	}

	//~ Inner Classes ----------------------------------------------------------

	/**
	 * Common base class for DML statement namespaces.
	 */
	public static class DmlNamespace extends IdentifierNamespace {
		protected DmlNamespace(SqlValidatorImpl validator, SqlNode id,
			SqlNode enclosingNode, SqlValidatorScope parentScope) {
			super(validator, id, enclosingNode, parentScope);
		}
	}

	/**
	 * Namespace for an INSERT statement.
	 */
	private static class InsertNamespace extends DmlNamespace {
		private final SqlInsert node;

		InsertNamespace(SqlValidatorImpl validator, SqlInsert node,
			SqlNode enclosingNode, SqlValidatorScope parentScope) {
			super(validator, node.getTargetTable(), enclosingNode, parentScope);
			this.node = Objects.requireNonNull(node);
		}

		public SqlInsert getNode() {
			return node;
		}
	}

	/**
	 * Namespace for an UPDATE statement.
	 */
	private static class UpdateNamespace extends DmlNamespace {
		private final SqlUpdate node;

		UpdateNamespace(SqlValidatorImpl validator, SqlUpdate node,
			SqlNode enclosingNode, SqlValidatorScope parentScope) {
			super(validator, node.getTargetTable(), enclosingNode, parentScope);
			this.node = Objects.requireNonNull(node);
		}

		public SqlUpdate getNode() {
			return node;
		}
	}

	/**
	 * Namespace for a DELETE statement.
	 */
	private static class DeleteNamespace extends DmlNamespace {
		private final SqlDelete node;

		DeleteNamespace(SqlValidatorImpl validator, SqlDelete node,
			SqlNode enclosingNode, SqlValidatorScope parentScope) {
			super(validator, node.getTargetTable(), enclosingNode, parentScope);
			this.node = Objects.requireNonNull(node);
		}

		public SqlDelete getNode() {
			return node;
		}
	}

	/**
	 * Namespace for a MERGE statement.
	 */
	private static class MergeNamespace extends DmlNamespace {
		private final SqlMerge node;

		MergeNamespace(SqlValidatorImpl validator, SqlMerge node,
			SqlNode enclosingNode, SqlValidatorScope parentScope) {
			super(validator, node.getTargetTable(), enclosingNode, parentScope);
			this.node = Objects.requireNonNull(node);
		}

		public SqlMerge getNode() {
			return node;
		}
	}

	/**
	 * retrieve pattern variables defined
	 */
	private class PatternVarVisitor implements SqlVisitor<Void> {
		private MatchRecognizeScope scope;
		PatternVarVisitor(MatchRecognizeScope scope) {
			this.scope = scope;
		}

		@Override public Void visit(SqlLiteral literal) {
			return null;
		}

		@Override public Void visit(SqlCall call) {
			for (int i = 0; i < call.getOperandList().size(); i++) {
				call.getOperandList().get(i).accept(this);
			}
			return null;
		}

		@Override public Void visit(SqlNodeList nodeList) {
			throw Util.needToImplement(nodeList);
		}

		@Override public Void visit(SqlIdentifier id) {
			Preconditions.checkArgument(id.isSimple());
			scope.addPatternVar(id.getSimple());
			return null;
		}

		@Override public Void visit(SqlDataTypeSpec type) {
			throw Util.needToImplement(type);
		}

		@Override public Void visit(SqlDynamicParam param) {
			throw Util.needToImplement(param);
		}

		@Override public Void visit(SqlIntervalQualifier intervalQualifier) {
			throw Util.needToImplement(intervalQualifier);
		}
	}

	/**
	 * Visitor which derives the type of a given {@link SqlNode}.
	 *
	 * <p>Each method must return the derived type. This visitor is basically a
	 * single-use dispatcher; the visit is never recursive.
	 */
	private class DeriveTypeVisitor implements SqlVisitor<RelDataType> {
		private final SqlValidatorScope scope;

		DeriveTypeVisitor(SqlValidatorScope scope) {
			this.scope = scope;
		}

		public RelDataType visit(SqlLiteral literal) {
			return literal.createSqlType(typeFactory);
		}

		public RelDataType visit(SqlCall call) {
			final SqlOperator operator = call.getOperator();
			return operator.deriveType(SqlValidatorImpl.this, scope, call);
		}

		public RelDataType visit(SqlNodeList nodeList) {
			// Operand is of a type that we can't derive a type for. If the
			// operand is of a peculiar type, such as a SqlNodeList, then you
			// should override the operator's validateCall() method so that it
			// doesn't try to validate that operand as an expression.
			throw Util.needToImplement(nodeList);
		}

		public RelDataType visit(SqlIdentifier id) {
			// First check for builtin functions which don't have parentheses,
			// like "LOCALTIME".
			final SqlCall call = makeNullaryCall(id);
			if (call != null) {
				return call.getOperator().validateOperands(
					SqlValidatorImpl.this,
					scope,
					call);
			}

			RelDataType type = null;
			if (!(scope instanceof EmptyScope)) {
				id = scope.fullyQualify(id).identifier;
			}

			// Resolve the longest prefix of id that we can
			int i;
			for (i = id.names.size() - 1; i > 0; i--) {
				// REVIEW jvs 9-June-2005: The name resolution rules used
				// here are supposed to match SQL:2003 Part 2 Section 6.6
				// (identifier chain), but we don't currently have enough
				// information to get everything right.  In particular,
				// routine parameters are currently looked up via resolve;
				// we could do a better job if they were looked up via
				// resolveColumn.

				final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
				final SqlValidatorScope.ResolvedImpl resolved =
					new SqlValidatorScope.ResolvedImpl();
				scope.resolve(id.names.subList(0, i), nameMatcher, false, resolved);
				if (resolved.count() == 1) {
					// There's a namespace with the name we seek.
					final SqlValidatorScope.Resolve resolve = resolved.only();
					type = resolve.rowType();
					for (SqlValidatorScope.Step p : Util.skip(resolve.path.steps())) {
						type = type.getFieldList().get(p.i).getType();
					}
					break;
				}
			}

			// Give precedence to namespace found, unless there
			// are no more identifier components.
			if (type == null || id.names.size() == 1) {
				// See if there's a column with the name we seek in
				// precisely one of the namespaces in this scope.
				RelDataType colType = scope.resolveColumn(id.names.get(0), id);
				if (colType != null) {
					type = colType;
				}
				++i;
			}

			if (type == null) {
				final SqlIdentifier last = id.getComponent(i - 1, i);
				throw newValidationError(last,
					RESOURCE.unknownIdentifier(last.toString()));
			}

			// Resolve rest of identifier
			for (; i < id.names.size(); i++) {
				String name = id.names.get(i);
				final RelDataTypeField field;
				if (name.equals("")) {
					// The wildcard "*" is represented as an empty name. It never
					// resolves to a field.
					name = "*";
					field = null;
				} else {
					final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
					field = nameMatcher.field(type, name);
				}
				if (field == null) {
					throw newValidationError(id.getComponent(i),
						RESOURCE.unknownField(name));
				}
				type = field.getType();
			}
			type =
				SqlTypeUtil.addCharsetAndCollation(
					type,
					getTypeFactory());
			return type;
		}

		public RelDataType visit(SqlDataTypeSpec dataType) {
			// Q. How can a data type have a type?
			// A. When it appears in an expression. (Say as the 2nd arg to the
			//    CAST operator.)
			validateDataType(dataType);
			return dataType.deriveType(SqlValidatorImpl.this);
		}

		public RelDataType visit(SqlDynamicParam param) {
			return unknownType;
		}

		public RelDataType visit(SqlIntervalQualifier intervalQualifier) {
			return typeFactory.createSqlIntervalType(intervalQualifier);
		}
	}

	/**
	 * Converts an expression into canonical form by fully-qualifying any
	 * identifiers.
	 */
	private static class Expander extends SqlScopedShuttle {
		protected final SqlValidatorImpl validator;

		Expander(SqlValidatorImpl validator, SqlValidatorScope scope) {
			super(scope);
			this.validator = validator;
		}

		@Override public SqlNode visit(SqlIdentifier id) {
			// First check for builtin functions which don't have
			// parentheses, like "LOCALTIME".
			final SqlCall call = validator.makeNullaryCall(id);
			if (call != null) {
				return call.accept(this);
			}
			final SqlIdentifier fqId = getScope().fullyQualify(id).identifier;
			SqlNode expandedExpr = expandDynamicStar(id, fqId);
			validator.setOriginal(expandedExpr, id);
			return expandedExpr;
		}

		@Override protected SqlNode visitScoped(SqlCall call) {
			switch (call.getKind()) {
				case SCALAR_QUERY:
				case CURRENT_VALUE:
				case NEXT_VALUE:
				case WITH:
					return call;
			}
			// Only visits arguments which are expressions. We don't want to
			// qualify non-expressions such as 'x' in 'empno * 5 AS x'.
			ArgHandler<SqlNode> argHandler =
				new CallCopyingArgHandler(call, false);
			call.getOperator().acceptCall(this, call, true, argHandler);
			final SqlNode result = argHandler.result();
			validator.setOriginal(result, call);
			return result;
		}

		protected SqlNode expandDynamicStar(SqlIdentifier id, SqlIdentifier fqId) {
			if (DynamicRecordType.isDynamicStarColName(Util.last(fqId.names))
				&& !DynamicRecordType.isDynamicStarColName(Util.last(id.names))) {
				// Convert a column ref into ITEM(*, 'col_name')
				// for a dynamic star field in dynTable's rowType.
				SqlNode[] inputs = new SqlNode[2];
				inputs[0] = fqId;
				inputs[1] = SqlLiteral.createCharString(
					Util.last(id.names),
					id.getParserPosition());
				return new SqlBasicCall(
					SqlStdOperatorTable.ITEM,
					inputs,
					id.getParserPosition());
			}
			return fqId;
		}
	}

	/**
	 * Shuttle which walks over an expression in the ORDER BY clause, replacing
	 * usages of aliases with the underlying expression.
	 */
	class OrderExpressionExpander extends SqlScopedShuttle {
		private final List<String> aliasList;
		private final SqlSelect select;
		private final SqlNode root;

		OrderExpressionExpander(SqlSelect select, SqlNode root) {
			super(getOrderScope(select));
			this.select = select;
			this.root = root;
			this.aliasList = getNamespace(select).getRowType().getFieldNames();
		}

		public SqlNode go() {
			return root.accept(this);
		}

		public SqlNode visit(SqlLiteral literal) {
			// Ordinal markers, e.g. 'select a, b from t order by 2'.
			// Only recognize them if they are the whole expression,
			// and if the dialect permits.
			if (literal == root && getConformance().isSortByOrdinal()) {
				switch (literal.getTypeName()) {
					case DECIMAL:
					case DOUBLE:
						final int intValue = literal.intValue(false);
						if (intValue >= 0) {
							if (intValue < 1 || intValue > aliasList.size()) {
								throw newValidationError(
									literal, RESOURCE.orderByOrdinalOutOfRange());
							}

							// SQL ordinals are 1-based, but Sort's are 0-based
							int ordinal = intValue - 1;
							return nthSelectItem(ordinal, literal.getParserPosition());
						}
						break;
				}
			}

			return super.visit(literal);
		}

		/**
		 * Returns the <code>ordinal</code>th item in the select list.
		 */
		private SqlNode nthSelectItem(int ordinal, final SqlParserPos pos) {
			// TODO: Don't expand the list every time. Maybe keep an expanded
			// version of each expression -- select lists and identifiers -- in
			// the validator.

			SqlNodeList expandedSelectList =
				expandStar(
					select.getSelectList(),
					select,
					false);
			SqlNode expr = expandedSelectList.get(ordinal);
			expr = stripAs(expr);
			if (expr instanceof SqlIdentifier) {
				expr = getScope().fullyQualify((SqlIdentifier) expr).identifier;
			}

			// Create a copy of the expression with the position of the order
			// item.
			return expr.clone(pos);
		}

		public SqlNode visit(SqlIdentifier id) {
			// Aliases, e.g. 'select a as x, b from t order by x'.
			if (id.isSimple()
				&& getConformance().isSortByAlias()) {
				String alias = id.getSimple();
				final SqlValidatorNamespace selectNs = getNamespace(select);
				final RelDataType rowType =
					selectNs.getRowTypeSansSystemColumns();
				final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
				RelDataTypeField field = nameMatcher.field(rowType, alias);
				if (field != null) {
					return nthSelectItem(
						field.getIndex(),
						id.getParserPosition());
				}
			}

			// No match. Return identifier unchanged.
			return getScope().fullyQualify(id).identifier;
		}

		protected SqlNode visitScoped(SqlCall call) {
			// Don't attempt to expand sub-queries. We haven't implemented
			// these yet.
			if (call instanceof SqlSelect) {
				return call;
			}
			return super.visitScoped(call);
		}
	}

	/**
	 * Shuttle which walks over an expression in the GROUP BY/HAVING clause, replacing
	 * usages of aliases or ordinals with the underlying expression.
	 */
	static class ExtendedExpander extends Expander {
		final SqlSelect select;
		final SqlNode root;
		final boolean havingExpr;

		ExtendedExpander(SqlValidatorImpl validator, SqlValidatorScope scope,
			SqlSelect select, SqlNode root, boolean havingExpr) {
			super(validator, scope);
			this.select = select;
			this.root = root;
			this.havingExpr = havingExpr;
		}

		@Override public SqlNode visit(SqlIdentifier id) {
			if (id.isSimple()
				&& (havingExpr
					    ? validator.getConformance().isHavingAlias()
					    : validator.getConformance().isGroupByAlias())) {
				String name = id.getSimple();
				SqlNode expr = null;
				final SqlNameMatcher nameMatcher =
					validator.catalogReader.nameMatcher();
				int n = 0;
				for (SqlNode s : select.getSelectList()) {
					final String alias = SqlValidatorUtil.getAlias(s, -1);
					if (alias != null && nameMatcher.matches(alias, name)) {
						expr = s;
						n++;
					}
				}
				if (n == 0) {
					return super.visit(id);
				} else if (n > 1) {
					// More than one column has this alias.
					throw validator.newValidationError(id,
						RESOURCE.columnAmbiguous(name));
				}
				if (havingExpr && validator.isAggregate(root)) {
					return super.visit(id);
				}
				expr = stripAs(expr);
				if (expr instanceof SqlIdentifier) {
					SqlIdentifier sid = (SqlIdentifier) expr;
					final SqlIdentifier fqId = getScope().fullyQualify(sid).identifier;
					expr = expandDynamicStar(sid, fqId);
				}
				return expr;
			}
			return super.visit(id);
		}

		public SqlNode visit(SqlLiteral literal) {
			if (havingExpr || !validator.getConformance().isGroupByOrdinal()) {
				return super.visit(literal);
			}
			boolean isOrdinalLiteral = literal == root;
			switch (root.getKind()) {
				case GROUPING_SETS:
				case ROLLUP:
				case CUBE:
					if (root instanceof SqlBasicCall) {
						List<SqlNode> operandList = ((SqlBasicCall) root).getOperandList();
						for (SqlNode node : operandList) {
							if (node.equals(literal)) {
								isOrdinalLiteral = true;
								break;
							}
						}
					}
					break;
			}
			if (isOrdinalLiteral) {
				switch (literal.getTypeName()) {
					case DECIMAL:
					case DOUBLE:
						final int intValue = literal.intValue(false);
						if (intValue >= 0) {
							if (intValue < 1 || intValue > select.getSelectList().size()) {
								throw validator.newValidationError(literal,
									RESOURCE.orderByOrdinalOutOfRange());
							}

							// SQL ordinals are 1-based, but Sort's are 0-based
							int ordinal = intValue - 1;
							return SqlUtil.stripAs(select.getSelectList().get(ordinal));
						}
						break;
				}
			}

			return super.visit(literal);
		}
	}

	/** Information about an identifier in a particular scope. */
	protected static class IdInfo {
		public final SqlValidatorScope scope;
		public final SqlIdentifier id;

		public IdInfo(SqlValidatorScope scope, SqlIdentifier id) {
			this.scope = scope;
			this.id = id;
		}
	}

	/**
	 * Utility object used to maintain information about the parameters in a
	 * function call.
	 */
	protected static class FunctionParamInfo {
		/**
		 * Maps a cursor (based on its position relative to other cursor
		 * parameters within a function call) to the SELECT associated with the
		 * cursor.
		 */
		public final Map<Integer, SqlSelect> cursorPosToSelectMap;

		/**
		 * Maps a column list parameter to the parent cursor parameter it
		 * references. The parameters are id'd by their names.
		 */
		public final Map<String, String> columnListParamToParentCursorMap;

		public FunctionParamInfo() {
			cursorPosToSelectMap = new HashMap<>();
			columnListParamToParentCursorMap = new HashMap<>();
		}
	}

	/**
	 * Modify the nodes in navigation function
	 * such as FIRST, LAST, PREV AND NEXT.
	 */
	private static class NavigationModifier extends SqlShuttle {
		public SqlNode go(SqlNode node) {
			return node.accept(this);
		}
	}

	/**
	 * Shuttle that expands navigation expressions in a MATCH_RECOGNIZE clause.
	 *
	 * <p>Examples:
	 *
	 * <ul>
	 *   <li>{@code PREV(A.price + A.amount)} &rarr;
	 *   {@code PREV(A.price) + PREV(A.amount)}
	 *
	 *   <li>{@code FIRST(A.price * 2)} &rarr; {@code FIRST(A.PRICE) * 2}
	 * </ul>
	 */
	private static class NavigationExpander extends NavigationModifier {
		final SqlOperator op;
		final SqlNode offset;

		NavigationExpander() {
			this(null, null);
		}

		NavigationExpander(SqlOperator operator, SqlNode offset) {
			this.offset = offset;
			this.op = operator;
		}

		@Override public SqlNode visit(SqlCall call) {
			SqlKind kind = call.getKind();
			List<SqlNode> operands = call.getOperandList();
			List<SqlNode> newOperands = new ArrayList<>();

			// This code is a workaround for CALCITE-2707
			if (call.getFunctionQuantifier() != null
				&& call.getFunctionQuantifier().getValue() == SqlSelectKeyword.DISTINCT) {
				final SqlParserPos pos = call.getParserPosition();
				throw SqlUtil.newContextException(pos, Static.RESOURCE.functionQuantifierNotAllowed(call.toString()));
			}
			// This code is a workaround for CALCITE-2707

			if (isLogicalNavigation(kind) || isPhysicalNavigation(kind)) {
				SqlNode inner = operands.get(0);
				SqlNode offset = operands.get(1);

				// merge two straight prev/next, update offset
				if (isPhysicalNavigation(kind)) {
					SqlKind innerKind = inner.getKind();
					if (isPhysicalNavigation(innerKind)) {
						List<SqlNode> innerOperands = ((SqlCall) inner).getOperandList();
						SqlNode innerOffset = innerOperands.get(1);
						SqlOperator newOperator = innerKind == kind
							? SqlStdOperatorTable.PLUS : SqlStdOperatorTable.MINUS;
						offset = newOperator.createCall(SqlParserPos.ZERO,
							offset, innerOffset);
						inner = call.getOperator().createCall(SqlParserPos.ZERO,
							innerOperands.get(0), offset);
					}
				}
				SqlNode newInnerNode =
					inner.accept(new NavigationExpander(call.getOperator(), offset));
				if (op != null) {
					newInnerNode = op.createCall(SqlParserPos.ZERO, newInnerNode,
						this.offset);
				}
				return newInnerNode;
			}

			if (operands.size() > 0) {
				for (SqlNode node : operands) {
					if (node != null) {
						SqlNode newNode = node.accept(new NavigationExpander());
						if (op != null) {
							newNode = op.createCall(SqlParserPos.ZERO, newNode, offset);
						}
						newOperands.add(newNode);
					} else {
						newOperands.add(null);
					}
				}
				return call.getOperator().createCall(SqlParserPos.ZERO, newOperands);
			} else {
				if (op == null) {
					return call;
				} else {
					return op.createCall(SqlParserPos.ZERO, call, offset);
				}
			}
		}

		@Override public SqlNode visit(SqlIdentifier id) {
			if (op == null) {
				return id;
			} else {
				return op.createCall(SqlParserPos.ZERO, id, offset);
			}
		}
	}

	/**
	 * Shuttle that replaces {@code A as A.price > PREV(B.price)} with
	 * {@code PREV(A.price, 0) > LAST(B.price, 0)}.
	 *
	 * <p>Replacing {@code A.price} with {@code PREV(A.price, 0)} makes the
	 * implementation of
	 * {@link RexVisitor#visitPatternFieldRef(RexPatternFieldRef)} more unified.
	 * Otherwise, it's difficult to implement this method. If it returns the
	 * specified field, then the navigation such as {@code PREV(A.price, 1)}
	 * becomes impossible; if not, then comparisons such as
	 * {@code A.price > PREV(A.price, 1)} become meaningless.
	 */
	private static class NavigationReplacer extends NavigationModifier {
		private final String alpha;

		NavigationReplacer(String alpha) {
			this.alpha = alpha;
		}

		@Override public SqlNode visit(SqlCall call) {
			SqlKind kind = call.getKind();
			if (isLogicalNavigation(kind)
				|| isAggregation(kind)
				|| isRunningOrFinal(kind)) {
				return call;
			}

			switch (kind) {
				case PREV:
					final List<SqlNode> operands = call.getOperandList();
					if (operands.get(0) instanceof SqlIdentifier) {
						String name = ((SqlIdentifier) operands.get(0)).names.get(0);
						return name.equals(alpha) ? call
							: SqlStdOperatorTable.LAST.createCall(SqlParserPos.ZERO, operands);
					}
			}
			return super.visit(call);
		}

		@Override public SqlNode visit(SqlIdentifier id) {
			if (id.isSimple()) {
				return id;
			}
			SqlOperator operator = id.names.get(0).equals(alpha)
				? SqlStdOperatorTable.PREV : SqlStdOperatorTable.LAST;

			return operator.createCall(SqlParserPos.ZERO, id,
				SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO));
		}
	}

	/**
	 * Within one navigation function, the pattern var should be same
	 */
	private class PatternValidator extends SqlBasicVisitor<Set<String>> {
		private final boolean isMeasure;
		int firstLastCount;
		int prevNextCount;
		int aggregateCount;

		PatternValidator(boolean isMeasure) {
			this(isMeasure, 0, 0, 0);
		}

		PatternValidator(boolean isMeasure, int firstLastCount, int prevNextCount,
			int aggregateCount) {
			this.isMeasure = isMeasure;
			this.firstLastCount = firstLastCount;
			this.prevNextCount = prevNextCount;
			this.aggregateCount = aggregateCount;
		}

		@Override public Set<String> visit(SqlCall call) {
			boolean isSingle = false;
			Set<String> vars = new HashSet<>();
			SqlKind kind = call.getKind();
			List<SqlNode> operands = call.getOperandList();

			if (isSingleVarRequired(kind)) {
				isSingle = true;
				if (isPhysicalNavigation(kind)) {
					if (isMeasure) {
						throw newValidationError(call,
							Static.RESOURCE.patternPrevFunctionInMeasure(call.toString()));
					}
					if (firstLastCount != 0) {
						throw newValidationError(call,
							Static.RESOURCE.patternPrevFunctionOrder(call.toString()));
					}
					prevNextCount++;
				} else if (isLogicalNavigation(kind)) {
					if (firstLastCount != 0) {
						throw newValidationError(call,
							Static.RESOURCE.patternPrevFunctionOrder(call.toString()));
					}
					firstLastCount++;
				} else if (isAggregation(kind)) {
					// cannot apply aggregation in PREV/NEXT, FIRST/LAST
					if (firstLastCount != 0 || prevNextCount != 0) {
						throw newValidationError(call,
							Static.RESOURCE.patternAggregationInNavigation(call.toString()));
					}
					if (kind == SqlKind.COUNT && call.getOperandList().size() > 1) {
						throw newValidationError(call,
							Static.RESOURCE.patternCountFunctionArg());
					}
					aggregateCount++;
				}
			}

			if (isRunningOrFinal(kind) && !isMeasure) {
				throw newValidationError(call,
					Static.RESOURCE.patternRunningFunctionInDefine(call.toString()));
			}

			for (SqlNode node : operands) {
				if (node != null) {
					vars.addAll(
						node.accept(
							new PatternValidator(isMeasure, firstLastCount, prevNextCount,
								aggregateCount)));
				}
			}

			if (isSingle) {
				switch (kind) {
					case COUNT:
						if (vars.size() > 1) {
							throw newValidationError(call,
								Static.RESOURCE.patternCountFunctionArg());
						}
						break;
					default:
						if (operands.size() == 0
							|| !(operands.get(0) instanceof SqlCall)
							|| ((SqlCall) operands.get(0)).getOperator() != SqlStdOperatorTable.CLASSIFIER) {
							if (vars.isEmpty()) {
								throw newValidationError(call,
									Static.RESOURCE.patternFunctionNullCheck(call.toString()));
							}
							if (vars.size() != 1) {
								throw newValidationError(call,
									Static.RESOURCE.patternFunctionVariableCheck(call.toString()));
							}
						}
						break;
				}
			}
			return vars;
		}

		@Override public Set<String> visit(SqlIdentifier identifier) {
			boolean check = prevNextCount > 0 || firstLastCount > 0 || aggregateCount > 0;
			Set<String> vars = new HashSet<>();
			if (identifier.names.size() > 1 && check) {
				vars.add(identifier.names.get(0));
			}
			return vars;
		}

		@Override public Set<String> visit(SqlLiteral literal) {
			return ImmutableSet.of();
		}

		@Override public Set<String> visit(SqlIntervalQualifier qualifier) {
			return ImmutableSet.of();
		}

		@Override public Set<String> visit(SqlDataTypeSpec type) {
			return ImmutableSet.of();
		}

		@Override public Set<String> visit(SqlDynamicParam param) {
			return ImmutableSet.of();
		}
	}

	/** Permutation of fields in NATURAL JOIN or USING. */
	private class Permute {
		final List<ImmutableIntList> sources;
		final RelDataType rowType;
		final boolean trivial;

		Permute(SqlNode from, int offset) {
			switch (from.getKind()) {
				case JOIN:
					final SqlJoin join = (SqlJoin) from;
					final Permute left = new Permute(join.getLeft(), offset);
					final int fieldCount =
						getValidatedNodeType(join.getLeft()).getFieldList().size();
					final Permute right =
						new Permute(join.getRight(), offset + fieldCount);
					final List<String> names = usingNames(join);
					final List<ImmutableIntList> sources = new ArrayList<>();
					final Set<ImmutableIntList> sourceSet = new HashSet<>();
					final RelDataTypeFactory.Builder b = typeFactory.builder();
					if (names != null) {
						for (String name : names) {
							final RelDataTypeField f = left.field(name);
							final ImmutableIntList source = left.sources.get(f.getIndex());
							sourceSet.add(source);
							final RelDataTypeField f2 = right.field(name);
							final ImmutableIntList source2 = right.sources.get(f2.getIndex());
							sourceSet.add(source2);
							sources.add(source.appendAll(source2));
							final boolean nullable =
								(f.getType().isNullable()
									 || join.getJoinType().generatesNullsOnLeft())
									&& (f2.getType().isNullable()
										    || join.getJoinType().generatesNullsOnRight());
							b.add(f).nullable(nullable);
						}
					}
					for (RelDataTypeField f : left.rowType.getFieldList()) {
						final ImmutableIntList source = left.sources.get(f.getIndex());
						if (sourceSet.add(source)) {
							sources.add(source);
							b.add(f);
						}
					}
					for (RelDataTypeField f : right.rowType.getFieldList()) {
						final ImmutableIntList source = right.sources.get(f.getIndex());
						if (sourceSet.add(source)) {
							sources.add(source);
							b.add(f);
						}
					}
					rowType = b.build();
					this.sources = ImmutableList.copyOf(sources);
					this.trivial = left.trivial
						&& right.trivial
						&& (names == null || names.isEmpty());
					break;

				default:
					rowType = getValidatedNodeType(from);
					this.sources = Functions.generate(rowType.getFieldCount(),
						i -> ImmutableIntList.of(offset + i));
					this.trivial = true;
			}
		}

		private RelDataTypeField field(String name) {
			return catalogReader.nameMatcher().field(rowType, name);
		}

		/** Returns the set of field names in the join condition specified by USING
		 * or implicitly by NATURAL, de-duplicated and in order. */
		private List<String> usingNames(SqlJoin join) {
			switch (join.getConditionType()) {
				case USING:
					final ImmutableList.Builder<String> list = ImmutableList.builder();
					final Set<String> names = catalogReader.nameMatcher().createSet();
					for (SqlNode node : (SqlNodeList) join.getCondition()) {
						final String name = ((SqlIdentifier) node).getSimple();
						if (names.add(name)) {
							list.add(name);
						}
					}
					return list.build();
				case NONE:
					if (join.isNatural()) {
						final RelDataType t0 = getValidatedNodeType(join.getLeft());
						final RelDataType t1 = getValidatedNodeType(join.getRight());
						return SqlValidatorUtil.deriveNaturalJoinColumnList(
							catalogReader.nameMatcher(), t0, t1);
					}
			}
			return null;
		}

		/** Moves fields according to the permutation. */
		public void permute(List<SqlNode> selectItems,
			List<Map.Entry<String, RelDataType>> fields) {
			if (trivial) {
				return;
			}

			final List<SqlNode> oldSelectItems = ImmutableList.copyOf(selectItems);
			selectItems.clear();
			final List<Map.Entry<String, RelDataType>> oldFields =
				ImmutableList.copyOf(fields);
			fields.clear();
			for (ImmutableIntList source : sources) {
				final int p0 = source.get(0);
				Map.Entry<String, RelDataType> field = oldFields.get(p0);
				final String name = field.getKey();
				RelDataType type = field.getValue();
				SqlNode selectItem = oldSelectItems.get(p0);
				for (int p1 : Util.skip(source)) {
					final Map.Entry<String, RelDataType> field1 = oldFields.get(p1);
					final SqlNode selectItem1 = oldSelectItems.get(p1);
					final RelDataType type1 = field1.getValue();
					// output is nullable only if both inputs are
					final boolean nullable = type.isNullable() && type1.isNullable();
					final RelDataType type2 =
						SqlTypeUtil.leastRestrictiveForComparison(typeFactory, type,
							type1);
					selectItem =
						SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
							SqlStdOperatorTable.COALESCE.createCall(SqlParserPos.ZERO,
								maybeCast(selectItem, type, type2),
								maybeCast(selectItem1, type1, type2)),
							new SqlIdentifier(name, SqlParserPos.ZERO));
					type = typeFactory.createTypeWithNullability(type2, nullable);
				}
				fields.add(Pair.of(name, type));
				selectItems.add(selectItem);
			}
		}
	}

	//~ Enums ------------------------------------------------------------------

	/**
	 * Validation status.
	 */
	public enum Status {
		/**
		 * Validation has not started for this scope.
		 */
		UNVALIDATED,

		/**
		 * Validation is in progress for this scope.
		 */
		IN_PROGRESS,

		/**
		 * Validation has completed (perhaps unsuccessfully).
		 */
		VALID
	}

}

// End SqlValidatorImpl.java
