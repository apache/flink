<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

/**
 * Parses a "IF EXISTS" option, default is false.
 */
boolean IfExistsOpt() :
{
}
{
    (
        LOOKAHEAD(2)
        <IF> <EXISTS> { return true; }
    |
        { return false; }
    )
}

/**
 * Parses a "IF NOT EXISTS" option, default is false.
 */
boolean IfNotExistsOpt() :
{
}
{
    (
        LOOKAHEAD(3)
        <IF> <NOT> <EXISTS> { return true; }
    |
        { return false; }
    )
}

/**
* Parse a "Show Catalogs" metadata query command.
*/
SqlShowCatalogs SqlShowCatalogs() :
{
}
{
    <SHOW> <CATALOGS>
    {
        return new SqlShowCatalogs(getPos());
    }
}

SqlCall SqlShowCurrentCatalogOrDatabase() :
{
}
{
    <SHOW> <CURRENT> ( <CATALOG>
        {
            return new SqlShowCurrentCatalog(getPos());
        }
    | <DATABASE>
        {
            return new SqlShowCurrentDatabase(getPos());
        }
    )
}

SqlDescribeCatalog SqlDescribeCatalog() :
{
    SqlIdentifier catalogName;
    SqlParserPos pos;
}
{
    ( <DESCRIBE> | <DESC> ) <CATALOG> { pos = getPos();}
    catalogName = SimpleIdentifier()
    {
        return new SqlDescribeCatalog(pos, catalogName);
    }

}

SqlUseCatalog SqlUseCatalog() :
{
    SqlIdentifier catalogName;
    SqlParserPos pos;
}
{
    <USE> <CATALOG> { pos = getPos();}
    catalogName = SimpleIdentifier()
    {
        return new SqlUseCatalog(pos, catalogName);
    }
}

/**
* Parses a create catalog statement.
* CREATE CATALOG catalog_name [WITH (property_name=property_value, ...)];
*/
SqlCreate SqlCreateCatalog(Span s, boolean replace) :
{
    SqlParserPos startPos;
    SqlIdentifier catalogName;
    SqlNodeList propertyList = SqlNodeList.EMPTY;
}
{
    <CATALOG> { startPos = getPos(); }
    catalogName = SimpleIdentifier()
    [
        <WITH>
        propertyList = TableProperties()
    ]
    {
        return new SqlCreateCatalog(startPos.plus(getPos()),
            catalogName,
            propertyList);
    }
}

SqlDrop SqlDropCatalog(Span s, boolean replace) :
{
    SqlIdentifier catalogName = null;
    boolean ifExists = false;
}
{
    <CATALOG>

    ifExists = IfExistsOpt()

    catalogName = CompoundIdentifier()

    {
        return new SqlDropCatalog(s.pos(), catalogName, ifExists);
    }
}

/**
* Parse a "Show DATABASES" metadata query command.
*/
SqlShowDatabases SqlShowDatabases() :
{
}
{
    <SHOW> <DATABASES>
    {
        return new SqlShowDatabases(getPos());
    }
}

SqlUseDatabase SqlUseDatabase() :
{
    SqlIdentifier databaseName;
    SqlParserPos pos;
}
{
    <USE> { pos = getPos();}
    databaseName = CompoundIdentifier()
    {
        return new SqlUseDatabase(pos, databaseName);
    }
}

/**
* Parses a create database statement.
* <pre>CREATE DATABASE database_name
*       [COMMENT database_comment]
*       [WITH (property_name=property_value, ...)].</pre>
*/
SqlCreate SqlCreateDatabase(Span s, boolean replace) :
{
    SqlParserPos startPos;
    SqlIdentifier databaseName;
    SqlCharStringLiteral comment = null;
    SqlNodeList propertyList = SqlNodeList.EMPTY;
    boolean ifNotExists = false;
}
{
    <DATABASE> { startPos = getPos(); }

    ifNotExists = IfNotExistsOpt()

    databaseName = CompoundIdentifier()
    [ <COMMENT> <QUOTED_STRING>
        {
            String p = SqlParserUtil.parseString(token.image);
            comment = SqlLiteral.createCharString(p, getPos());
        }
    ]
    [
        <WITH>
        propertyList = TableProperties()
    ]

    { return new SqlCreateDatabase(startPos.plus(getPos()),
                    databaseName,
                    propertyList,
                    comment,
                    ifNotExists); }

}

SqlAlterDatabase SqlAlterDatabase() :
{
    SqlParserPos startPos;
    SqlIdentifier databaseName;
    SqlNodeList propertyList = SqlNodeList.EMPTY;
}
{
    <ALTER> <DATABASE> { startPos = getPos(); }
    databaseName = CompoundIdentifier()
    <SET>
    propertyList = TableProperties()
    {
        return new SqlAlterDatabase(startPos.plus(getPos()),
                    databaseName,
                    propertyList);
    }
}

SqlDrop SqlDropDatabase(Span s, boolean replace) :
{
    SqlIdentifier databaseName = null;
    boolean ifExists = false;
    boolean cascade = false;
}
{
    <DATABASE>

    ifExists = IfExistsOpt()

    databaseName = CompoundIdentifier()
    [
        <RESTRICT> { cascade = false; }
    |
        <CASCADE>  { cascade = true; }
    ]

    {
         return new SqlDropDatabase(s.pos(), databaseName, ifExists, cascade);
    }
}

SqlDescribeDatabase SqlDescribeDatabase() :
{
    SqlIdentifier databaseName;
    SqlParserPos pos;
    boolean isExtended = false;
}
{
    ( <DESCRIBE> | <DESC> ) <DATABASE> { pos = getPos();}
    [ <EXTENDED> { isExtended = true;} ]
    databaseName = CompoundIdentifier()
    {
        return new SqlDescribeDatabase(pos, databaseName, isExtended);
    }

}

SqlCreate SqlCreateFunction(Span s, boolean replace, boolean isTemporary) :
{
    SqlIdentifier functionIdentifier = null;
    SqlCharStringLiteral functionClassName = null;
    String functionLanguage = null;
    boolean ifNotExists = false;
    boolean isSystemFunction = false;
    SqlNodeList resourceInfos = SqlNodeList.EMPTY;
    SqlParserPos functionLanguagePos = null;
}
{
    (
        <SYSTEM>
        {
            if (!isTemporary){
                throw SqlUtil.newContextException(getPos(),
                    ParserResource.RESOURCE.createSystemFunctionOnlySupportTemporary());
            }
        }
        <FUNCTION>
        ifNotExists = IfNotExistsOpt()
        functionIdentifier = SimpleIdentifier()
        { isSystemFunction = true; }
    |
        <FUNCTION>
        ifNotExists = IfNotExistsOpt()
        functionIdentifier = CompoundIdentifier()
    )

    <AS> <QUOTED_STRING> {
        String p = SqlParserUtil.parseString(token.image);
        functionClassName = SqlLiteral.createCharString(p, getPos());
    }
    [
        <LANGUAGE>
        (
            <JAVA> {
                functionLanguage = "JAVA";
                functionLanguagePos = getPos();
            }
        |
            <SCALA> {
                functionLanguage = "SCALA";
                functionLanguagePos = getPos();
            }
        |
            <SQL> {
                functionLanguage = "SQL";
                functionLanguagePos = getPos();
            }
        |
            <PYTHON> {
                functionLanguage = "PYTHON";
                functionLanguagePos = getPos();
            }
        )
    ]
    [
        <USING> {
            if ("SQL".equals(functionLanguage) || "PYTHON".equals(functionLanguage)) {
                throw SqlUtil.newContextException(
                    functionLanguagePos,
                    ParserResource.RESOURCE.createFunctionUsingJar(functionLanguage));
            }
            List<SqlNode> resourceList = new ArrayList<SqlNode>();
            SqlResource sqlResource = null;
        }
        sqlResource = SqlResourceInfo() {
            resourceList.add(sqlResource);
        }
        (
            <COMMA>
            sqlResource = SqlResourceInfo() {
                resourceList.add(sqlResource);
            }
        )*
        {  resourceInfos = new SqlNodeList(resourceList, s.pos()); }
    ]
    {
        return new SqlCreateFunction(
                    s.pos(),
                    functionIdentifier,
                    functionClassName,
                    functionLanguage,
                    ifNotExists,
                    isTemporary,
                    isSystemFunction,
                    resourceInfos);
    }
}

/**
 * Parse resource type and path.
 */
SqlResource SqlResourceInfo() :
{
    String resourcePath;
}
{
    <JAR> <QUOTED_STRING> {
        resourcePath = SqlParserUtil.parseString(token.image);
        return new SqlResource(
                    getPos(),
                    SqlResourceType.JAR.symbol(getPos()),
                    SqlLiteral.createCharString(resourcePath, getPos()));
    }
}

SqlDrop SqlDropFunction(Span s, boolean replace, boolean isTemporary) :
{
    SqlIdentifier functionIdentifier = null;
    boolean ifExists = false;
    boolean isSystemFunction = false;
}
{
    [ <SYSTEM>   { isSystemFunction = true; } ]

    <FUNCTION>

    ifExists = IfExistsOpt()

    functionIdentifier = CompoundIdentifier()

    {
        return new SqlDropFunction(s.pos(), functionIdentifier, ifExists, isTemporary, isSystemFunction);
    }
}

SqlAlterFunction SqlAlterFunction() :
{
    SqlIdentifier functionIdentifier = null;
    SqlCharStringLiteral functionClassName = null;
    String functionLanguage = null;
    SqlParserPos startPos;
    boolean ifExists = false;
    boolean isTemporary = false;
    boolean isSystemFunction = false;
}
{
    <ALTER>

    [ <TEMPORARY> { isTemporary = true; }
        [  <SYSTEM>   { isSystemFunction = true; } ]
    ]

    <FUNCTION> { startPos = getPos(); }

    ifExists = IfExistsOpt()

    functionIdentifier = CompoundIdentifier()

    <AS> <QUOTED_STRING> {
        String p = SqlParserUtil.parseString(token.image);
        functionClassName = SqlLiteral.createCharString(p, getPos());
    }

    [<LANGUAGE>
        (   <JAVA>  { functionLanguage = "JAVA"; }
        |
            <SCALA> { functionLanguage = "SCALA"; }
        |
            <SQL>   { functionLanguage = "SQL"; }
        |
            <PYTHON>   { functionLanguage = "PYTHON"; }
        )
    ]
    {
        return new SqlAlterFunction(startPos.plus(getPos()), functionIdentifier, functionClassName,
            functionLanguage, ifExists, isTemporary, isSystemFunction);
    }
}

/**
* Parses a show functions statement.
* SHOW [USER] FUNCTIONS;
*/
SqlShowFunctions SqlShowFunctions() :
{
    SqlParserPos pos;
    boolean requireUser = false;
}
{
    <SHOW> { pos = getPos();}
    [
        <USER> { requireUser = true; }
    ]
    <FUNCTIONS>
    {
        return new SqlShowFunctions(pos.plus(getPos()), requireUser);
    }
}

/**
 * Parse a "Show Views" metadata query command.
 */
SqlShowViews SqlShowViews() :
{
    SqlParserPos pos;
}
{
    <SHOW> <VIEWS> { pos = getPos(); }
    {
        return new SqlShowViews(pos);
    }
}

/**
* SHOW TABLES FROM [catalog.] database sql call.
*/
SqlShowTables SqlShowTables() :
{
    SqlIdentifier databaseName = null;
    SqlCharStringLiteral likeLiteral = null;
    String prep = null;
    boolean notLike = false;
    SqlParserPos pos;
}
{
    <SHOW> <TABLES>
    { pos = getPos(); }
    [
        ( <FROM> { prep = "FROM"; } | <IN> { prep = "IN"; } )
        { pos = getPos(); }
        databaseName = CompoundIdentifier()
    ]
    [
        [
            <NOT>
            {
                notLike = true;
            }
        ]
        <LIKE>  <QUOTED_STRING>
        {
            String likeCondition = SqlParserUtil.parseString(token.image);
            likeLiteral = SqlLiteral.createCharString(likeCondition, getPos());
        }
    ]
    {
        return new SqlShowTables(pos, prep, databaseName, notLike, likeLiteral);
    }
}

/**
* SHOW COLUMNS FROM [[catalog.] database.]sqlIdentifier sql call.
*/
SqlShowColumns SqlShowColumns() :
{
    SqlIdentifier tableName;
    SqlCharStringLiteral likeLiteral = null;
    String prep = "FROM";
    boolean notLike = false;
    SqlParserPos pos;
}
{
    <SHOW> <COLUMNS> ( <FROM> | <IN> { prep = "IN"; } )
    { pos = getPos();}
    tableName = CompoundIdentifier()
    [
        [
            <NOT>
            {
                notLike = true;
            }
        ]
        <LIKE>  <QUOTED_STRING>
        {
            String likeCondition = SqlParserUtil.parseString(token.image);
            likeLiteral = SqlLiteral.createCharString(likeCondition, getPos());
        }
    ]
    {
        return new SqlShowColumns(pos, prep, tableName, notLike, likeLiteral);
    }
}

/**
* Parse a "Show Create Table" query and "Show Create View" query commands.
*/
SqlShowCreate SqlShowCreate() :
{
    SqlIdentifier sqlIdentifier;
    SqlParserPos pos;
}
{
    <SHOW> <CREATE>
    (
        <TABLE>
        { pos = getPos(); }
        sqlIdentifier = CompoundIdentifier()
        {
            return new SqlShowCreateTable(pos, sqlIdentifier);
        }
    |
        <VIEW>
        { pos = getPos(); }
        sqlIdentifier = CompoundIdentifier()
        {
            return new SqlShowCreateView(pos, sqlIdentifier);
        }
    )
}

/**
 * DESCRIBE | DESC [ EXTENDED] [[catalogName.] dataBasesName].tableName sql call.
 * Here we add Rich in className to distinguish from calcite's original SqlDescribeTable.
 */
SqlRichDescribeTable SqlRichDescribeTable() :
{
    SqlIdentifier tableName;
    SqlParserPos pos;
    boolean isExtended = false;
}
{
    ( <DESCRIBE> | <DESC> ) { pos = getPos();}
    [ <EXTENDED> { isExtended = true;} ]
    tableName = CompoundIdentifier()
    {
        return new SqlRichDescribeTable(pos, tableName, isExtended);
    }
}

SqlAlterTable SqlAlterTable() :
{
    SqlParserPos startPos;
    SqlIdentifier tableIdentifier;
    SqlIdentifier newTableIdentifier = null;
    SqlNodeList propertyList = SqlNodeList.EMPTY;
    SqlNodeList propertyKeyList = SqlNodeList.EMPTY;
    SqlNodeList partitionSpec = null;
    SqlIdentifier constraintName;
    AlterTableContext ctx = new AlterTableContext();
}
{
    <ALTER> <TABLE> { startPos = getPos(); }
        tableIdentifier = CompoundIdentifier()
    (
        <RENAME> <TO>
        newTableIdentifier = CompoundIdentifier()
        {
            return new SqlAlterTableRename(
                        startPos.plus(getPos()),
                        tableIdentifier,
                        newTableIdentifier);
        }
    |
        <RESET>
        propertyKeyList = TablePropertyKeys()
        {
            return new SqlAlterTableReset(
                        startPos.plus(getPos()),
                        tableIdentifier,
                        propertyKeyList);
        }
    |
        <SET>
        propertyList = TableProperties()
        {
            return new SqlAlterTableOptions(
                        startPos.plus(getPos()),
                        tableIdentifier,
                        propertyList);
        }
    |
        <ADD>
        (
            AlterTableAddOrModify(ctx) {
                // TODO: remove it after supports convert SqlNode to Operation,
                // the jira link https://issues.apache.org/jira/browse/FLINK-22315
                if (ctx.constraints.size() > 0) {
                    return new SqlAlterTableAddConstraint(
                                tableIdentifier,
                                ctx.constraints.get(0),
                                startPos.plus(getPos()));
                }
            }
        |
            <LPAREN>
            AlterTableAddOrModify(ctx)
            (
                <COMMA> AlterTableAddOrModify(ctx)
            )*
            <RPAREN>
        )
        {
            return new SqlAlterTableAdd(
                        startPos.plus(getPos()),
                        tableIdentifier,
                        new SqlNodeList(ctx.columnPositions, startPos.plus(getPos())),
                        ctx.constraints,
                        ctx.watermark);
        }
    |
        <MODIFY>
        (
            AlterTableAddOrModify(ctx)
        |
            <LPAREN>
            AlterTableAddOrModify(ctx)
            (
                <COMMA> AlterTableAddOrModify(ctx)
            )*
            <RPAREN>
        )
        {
            return new SqlAlterTableModify(
                        startPos.plus(getPos()),
                        tableIdentifier,
                        new SqlNodeList(ctx.columnPositions, startPos.plus(getPos())),
                        ctx.constraints,
                        ctx.watermark);
        }

    |
        <DROP> <CONSTRAINT>
        constraintName = SimpleIdentifier() {
            return new SqlAlterTableDropConstraint(
                tableIdentifier,
                constraintName,
                startPos.plus(getPos()));
        }
    |
        [
            <PARTITION>
            {   partitionSpec = new SqlNodeList(getPos());
                PartitionSpecCommaList(partitionSpec);
            }
        ]
        <COMPACT>
        {
            return new SqlAlterTableCompact(startPos.plus(getPos()), tableIdentifier, partitionSpec);
        }
    )
}

/** Parse a table option key list. */
SqlNodeList TablePropertyKeys():
{
    SqlNode key;
    final List<SqlNode> proKeyList = new ArrayList<SqlNode>();
    final Span span;
}
{
    <LPAREN> { span = span(); }
    [
        key = StringLiteral()
        {
            proKeyList.add(key);
        }
        (
            <COMMA> key = StringLiteral()
            {
                proKeyList.add(key);
            }
        )*
    ]
    <RPAREN>
    {  return new SqlNodeList(proKeyList, span.end(this)); }
}

void TableColumn(TableCreationContext context) :
{
    SqlTableConstraint constraint;
}
{
    (
        LOOKAHEAD(2)
        TypedColumn(context)
    |
        constraint = TableConstraint() {
            context.constraints.add(constraint);
        }
    |
        ComputedColumn(context)
    |
        Watermark(context)
    )
}

void Watermark(TableCreationContext context) :
{
    SqlIdentifier eventTimeColumnName;
    SqlParserPos pos;
    SqlNode watermarkStrategy;
}
{
    <WATERMARK> {pos = getPos();} <FOR>
    eventTimeColumnName = CompoundIdentifier()
    <AS>
    watermarkStrategy = Expression(ExprContext.ACCEPT_NON_QUERY) {
        if (context.watermark != null) {
            throw SqlUtil.newContextException(pos,
                ParserResource.RESOURCE.multipleWatermarksUnsupported());
        } else {
            context.watermark = new SqlWatermark(pos, eventTimeColumnName, watermarkStrategy);
        }
    }
}

/** Parses {@code column_name column_data_type [...]}. */
SqlTableColumn TypedColumn(TableCreationContext context) :
{
    SqlTableColumn tableColumn;
    SqlIdentifier name;
    SqlParserPos pos;
    SqlDataTypeSpec type;
}
{
    name = SimpleIdentifier() {pos = getPos();}
    type = ExtendedDataType()
    (
        tableColumn = MetadataColumn(context, name, type)
    |
        tableColumn = RegularColumn(context, name, type)
    )
    {
        return tableColumn;
    }
}

/** Parses {@code column_name AS expr [COMMENT 'comment']}. */
SqlTableColumn ComputedColumn(TableCreationContext context) :
{
    SqlIdentifier name;
    SqlParserPos pos;
    SqlNode expr;
    SqlNode comment = null;
}
{
    name = SimpleIdentifier() {pos = getPos();}
    <AS>
    expr = Expression(ExprContext.ACCEPT_NON_QUERY)
    [
        <COMMENT>
        comment = StringLiteral()
    ]
    {
        SqlTableColumn computedColumn = new SqlTableColumn.SqlComputedColumn(
            getPos(),
            name,
            comment,
            expr);
        context.columnList.add(computedColumn);
        return computedColumn;
    }
}

/** Parses {@code column_name column_data_type METADATA [FROM 'alias_name'] [VIRTUAL] [COMMENT 'comment']}. */
SqlTableColumn MetadataColumn(TableCreationContext context, SqlIdentifier name, SqlDataTypeSpec type) :
{
    SqlNode metadataAlias = null;
    boolean isVirtual = false;
    SqlNode comment = null;
}
{
    <METADATA>
    [
        <FROM>
        metadataAlias = StringLiteral()
    ]
    [
        <VIRTUAL> {
            isVirtual = true;
        }
    ]
    [
        <COMMENT>
        comment = StringLiteral()
    ]
    {
        SqlTableColumn metadataColumn = new SqlTableColumn.SqlMetadataColumn(
            getPos(),
            name,
            comment,
            type,
            metadataAlias,
            isVirtual);
        context.columnList.add(metadataColumn);
        return metadataColumn;
    }
}

/** Parses {@code column_name column_data_type [constraint] [COMMENT 'comment']}. */
SqlTableColumn RegularColumn(TableCreationContext context, SqlIdentifier name, SqlDataTypeSpec type) :
{
    SqlTableConstraint constraint = null;
    SqlNode comment = null;
}
{
    [
        constraint = ColumnConstraint(name)
    ]
    [
        <COMMENT>
        comment = StringLiteral()
    ]
    {
        SqlTableColumn regularColumn = new SqlTableColumn.SqlRegularColumn(
            getPos(),
            name,
            comment,
            type,
            constraint);
        context.columnList.add(regularColumn);
        return regularColumn;
    }
}

/** Parses {@code ALTER TABLE table_name ADD/MODIFY [...]}. */
void AlterTableAddOrModify(AlterTableContext context) :
{
    SqlTableConstraint constraint;
}
{
    (
        AddOrModifyColumn(context)
    |
        constraint = TableConstraint() {
            context.constraints.add(constraint);
        }
    |
        Watermark(context)
    )
}

/** Parses {@code ADD/MODIFY column_name column_data_type [...]}. */
void AddOrModifyColumn(AlterTableContext context) :
{
    SqlTableColumn column;
    SqlIdentifier referencedColumn = null;
    SqlTableColumnPosition columnPos = null;
}
{
    (
        LOOKAHEAD(2)
        column = TypedColumn(context)
    |
        column = ComputedColumn(context)
    )
    [
        (
            <AFTER>
            referencedColumn = SimpleIdentifier()
            {
                columnPos = new SqlTableColumnPosition(
                    getPos(),
                    column,
                    SqlColumnPosSpec.AFTER.symbol(getPos()),
                    referencedColumn);
            }
        |
            <FIRST>
            {
                columnPos = new SqlTableColumnPosition(
                    getPos(),
                    column,
                    SqlColumnPosSpec.FIRST.symbol(getPos()),
                    referencedColumn);
            }
        )
    ]
    {
        if (columnPos == null) {
            columnPos = new SqlTableColumnPosition(
                getPos(),
                column,
                null,
                referencedColumn);
        }
        context.columnPositions.add(columnPos);
    }
}

/**
* Different with {@link #DataType()}, we support a [ NULL | NOT NULL ] suffix syntax for both the
* collection element data type and the data type itself.
*
* <p>See {@link #SqlDataTypeSpec} for the syntax details of {@link #DataType()}.
*/
SqlDataTypeSpec ExtendedDataType() :
{
    SqlTypeNameSpec typeName;
    final Span s;
    boolean elementNullable = true;
    boolean nullable = true;
}
{
    <#-- #DataType does not take care of the nullable attribute. -->
    typeName = TypeName() {
        s = span();
    }
    (
        LOOKAHEAD(3)
        elementNullable = NullableOptDefaultTrue()
        typeName = ExtendedCollectionsTypeName(typeName, elementNullable)
    )*
    nullable = NullableOptDefaultTrue()
    {
        return new SqlDataTypeSpec(typeName, s.end(this)).withNullable(nullable);
    }
}

/** Parses a column constraint for CREATE TABLE. */
SqlTableConstraint ColumnConstraint(SqlIdentifier column) :
{
    SqlIdentifier constraintName = null;
    final SqlLiteral uniqueSpec;
    SqlLiteral enforcement = null;
}
{

    [ constraintName = ConstraintName() ]
    uniqueSpec = UniqueSpec()
    [ enforcement = ConstraintEnforcement() ]
    {
        return new SqlTableConstraint(
                        constraintName,
                        uniqueSpec,
                        SqlNodeList.of(column),
                        enforcement,
                        false,
                        getPos());
    }
}

/** Parses a table constraint for CREATE TABLE. */
SqlTableConstraint TableConstraint() :
{
    SqlIdentifier constraintName = null;
    final SqlLiteral uniqueSpec;
    final SqlNodeList columns;
    SqlLiteral enforcement = null;
}
{

    [ constraintName = ConstraintName() ]
    uniqueSpec = UniqueSpec()
    columns = ParenthesizedSimpleIdentifierList()
    [ enforcement = ConstraintEnforcement() ]
    {
        return new SqlTableConstraint(
                        constraintName,
                        uniqueSpec,
                        columns,
                        enforcement,
                        true,
                        getPos());
    }
}

SqlIdentifier ConstraintName() :
{
    SqlIdentifier constraintName;
}
{
    <CONSTRAINT> constraintName = SimpleIdentifier() {
        return constraintName;
    }
}

SqlLiteral UniqueSpec() :
{
    SqlLiteral uniqueSpec;
}
{
    (
    <PRIMARY> <KEY> {
            uniqueSpec = SqlUniqueSpec.PRIMARY_KEY.symbol(getPos());
        }
    |
    <UNIQUE> {
            uniqueSpec = SqlUniqueSpec.UNIQUE.symbol(getPos());
        }
    )
    {
        return uniqueSpec;
    }
}

SqlLiteral ConstraintEnforcement() :
{
    SqlLiteral enforcement;
}
{
    (
    <ENFORCED> {
            enforcement = SqlConstraintEnforcement.ENFORCED.symbol(getPos());
        }
    |
    <NOT> <ENFORCED> {
            enforcement = SqlConstraintEnforcement.NOT_ENFORCED.symbol(getPos());
        }
    )
    {
        return enforcement;
    }
}

SqlNode TableOption() :
{
    SqlNode key;
    SqlNode value;
    SqlParserPos pos;
}
{
    key = StringLiteral()
    { pos = getPos(); }
    <EQ> value = StringLiteral()
    {
        return new SqlTableOption(key, value, getPos());
    }
}

/** Parse a table properties. */
SqlNodeList TableProperties():
{
    SqlNode property;
    final List<SqlNode> proList = new ArrayList<SqlNode>();
    final Span span;
}
{
    <LPAREN> { span = span(); }
    [
        property = TableOption()
        {
            proList.add(property);
        }
        (
            <COMMA> property = TableOption()
            {
                proList.add(property);
            }
        )*
    ]
    <RPAREN>
    {  return new SqlNodeList(proList, span.end(this)); }
}

SqlCreate SqlCreateTable(Span s, boolean replace, boolean isTemporary) :
{
    final SqlParserPos startPos = s.pos();
    boolean ifNotExists = false;
    SqlIdentifier tableName;
    List<SqlTableConstraint> constraints = new ArrayList<SqlTableConstraint>();
    SqlWatermark watermark = null;
    SqlNodeList columnList = SqlNodeList.EMPTY;
	SqlCharStringLiteral comment = null;
	SqlTableLike tableLike = null;
    SqlNode asQuery = null;

    SqlNodeList propertyList = SqlNodeList.EMPTY;
    SqlNodeList partitionColumns = SqlNodeList.EMPTY;
    SqlParserPos pos = startPos;
}
{
    <TABLE>

    ifNotExists = IfNotExistsOpt()

    tableName = CompoundIdentifier()
    [
        <LPAREN> { pos = getPos(); TableCreationContext ctx = new TableCreationContext();}
        TableColumn(ctx)
        (
            <COMMA> TableColumn(ctx)
        )*
        {
            pos = pos.plus(getPos());
            columnList = new SqlNodeList(ctx.columnList, pos);
            constraints = ctx.constraints;
            watermark = ctx.watermark;
        }
        <RPAREN>
    ]
    [ <COMMENT> <QUOTED_STRING> {
        String p = SqlParserUtil.parseString(token.image);
        comment = SqlLiteral.createCharString(p, getPos());
    }]
    [
        <PARTITIONED> <BY>
        partitionColumns = ParenthesizedSimpleIdentifierList()
    ]
    [
        <WITH>
        propertyList = TableProperties()
    ]
    [
        <LIKE>
        tableLike = SqlTableLike(getPos())
        {
            return new SqlCreateTableLike(startPos.plus(getPos()),
                tableName,
                columnList,
                constraints,
                propertyList,
                partitionColumns,
                watermark,
                comment,
                tableLike,
                isTemporary,
                ifNotExists);
        }
    |
        <AS>
        asQuery = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
        {
            return new SqlCreateTableAs(startPos.plus(getPos()),
                tableName,
                columnList,
                constraints,
                propertyList,
                partitionColumns,
                watermark,
                comment,
                asQuery,
                isTemporary,
                ifNotExists);
        }
    ]
    {
        return new SqlCreateTable(startPos.plus(getPos()),
            tableName,
            columnList,
            constraints,
            propertyList,
            partitionColumns,
            watermark,
            comment,
            isTemporary,
            ifNotExists);
    }
}

SqlTableLike SqlTableLike(SqlParserPos startPos):
{
    final List<SqlTableLikeOption> likeOptions = new ArrayList<SqlTableLikeOption>();
    SqlIdentifier tableName;
    SqlTableLikeOption likeOption;
}
{
    tableName = CompoundIdentifier()
    [
        <LPAREN>
        (
            likeOption = SqlTableLikeOption()
            {
                likeOptions.add(likeOption);
            }
        )+
        <RPAREN>
    ]
    {
        return new SqlTableLike(
            startPos.plus(getPos()),
            tableName,
            likeOptions
        );
    }
}

SqlTableLikeOption SqlTableLikeOption():
{
    MergingStrategy mergingStrategy;
    FeatureOption featureOption;
}
{
    (
        <INCLUDING> { mergingStrategy = MergingStrategy.INCLUDING; }
    |
        <EXCLUDING> { mergingStrategy = MergingStrategy.EXCLUDING; }
    |
        <OVERWRITING> { mergingStrategy = MergingStrategy.OVERWRITING; }
    )
    (
        <ALL> { featureOption = FeatureOption.ALL;}
    |
        <CONSTRAINTS> { featureOption = FeatureOption.CONSTRAINTS;}
    |
        <GENERATED> { featureOption = FeatureOption.GENERATED;}
    |
        <METADATA> { featureOption = FeatureOption.METADATA;}
    |
        <OPTIONS> { featureOption = FeatureOption.OPTIONS;}
    |
        <PARTITIONS> { featureOption = FeatureOption.PARTITIONS;}
    |
        <WATERMARKS> { featureOption = FeatureOption.WATERMARKS;}
    )

    {
        return new SqlTableLikeOption(mergingStrategy, featureOption);
    }
}

SqlDrop SqlDropTable(Span s, boolean replace, boolean isTemporary) :
{
    SqlIdentifier tableName = null;
    boolean ifExists = false;
}
{
    <TABLE>

    ifExists = IfExistsOpt()

    tableName = CompoundIdentifier()

    {
         return new SqlDropTable(s.pos(), tableName, ifExists, isTemporary);
    }
}

/**
* Parses an INSERT statement.
*/
SqlNode RichSqlInsert() :
{
    final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    final SqlNodeList keywordList;
    final List<SqlLiteral> extendedKeywords = new ArrayList<SqlLiteral>();
    final SqlNodeList extendedKeywordList;
    SqlNode table;
    SqlNodeList extendList = null;
    SqlNode source;
    final SqlNodeList partitionList = new SqlNodeList(getPos());
    SqlNodeList columnList = null;
    final Span s;
}
{
    (
        <INSERT>
    |
        <UPSERT> { keywords.add(SqlInsertKeyword.UPSERT.symbol(getPos())); }
    )
    (
        <INTO>
    |
        <OVERWRITE> {
            if (RichSqlInsert.isUpsert(keywords)) {
                throw SqlUtil.newContextException(getPos(),
                    ParserResource.RESOURCE.overwriteIsOnlyUsedWithInsert());
            }
            extendedKeywords.add(RichSqlInsertKeyword.OVERWRITE.symbol(getPos()));
        }
    )
    { s = span(); }
    SqlInsertKeywords(keywords) {
        keywordList = new SqlNodeList(keywords, s.addAll(keywords).pos());
        extendedKeywordList = new SqlNodeList(extendedKeywords, s.addAll(extendedKeywords).pos());
    }
    table = TableRefWithHintsOpt()
    [
        LOOKAHEAD(5)
        [ <EXTEND> ]
        extendList = ExtendList() {
            table = extend(table, extendList);
        }
    ]
    [
        <PARTITION> PartitionSpecCommaList(partitionList)
    ]
    [
        LOOKAHEAD(2)
        { final Pair<SqlNodeList, SqlNodeList> p; }
        p = ParenthesizedCompoundIdentifierList() {
            if (p.right.size() > 0) {
                table = extend(table, p.right);
            }
            if (p.left.size() > 0) {
                columnList = p.left;
            }
        }
    ]
    source = RichOrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return new RichSqlInsert(s.end(source), keywordList, extendedKeywordList, table, source,
            columnList, partitionList);
    }
}

/**
* Parses a partition specifications statement,
* e.g. insert into tbl1 partition(col1='val1', col2='val2') select col3 from tbl.
*/
void PartitionSpecCommaList(SqlNodeList list) :
{
    SqlIdentifier key;
    SqlNode value;
    SqlParserPos pos;
}
{
    <LPAREN>
    key = SimpleIdentifier()
    { pos = getPos(); }
    <EQ> value = Literal() {
        list.add(new SqlProperty(key, value, pos));
    }
    (
        <COMMA> key = SimpleIdentifier() { pos = getPos(); }
        <EQ> value = Literal() {
            list.add(new SqlProperty(key, value, pos));
        }
    )*
    <RPAREN>
}

/**
* Parses a create view or temporary view statement.
*   CREATE [OR REPLACE] [TEMPORARY] VIEW [IF NOT EXISTS] view_name [ (field1, field2 ...) ]
*   AS select_statement
* We only support [IF NOT EXISTS] semantic in Flink although the parser supports [OR REPLACE] grammar.
* See: FLINK-17067
*/
SqlCreate SqlCreateView(Span s, boolean replace, boolean isTemporary) : {
    SqlIdentifier viewName;
    SqlCharStringLiteral comment = null;
    SqlNode query;
    SqlNodeList fieldList = SqlNodeList.EMPTY;
    boolean ifNotExists = false;
}
{
    <VIEW>

    ifNotExists = IfNotExistsOpt()

    viewName = CompoundIdentifier()
    [
        fieldList = ParenthesizedSimpleIdentifierList()
    ]
    [ <COMMENT> <QUOTED_STRING> {
            String p = SqlParserUtil.parseString(token.image);
            comment = SqlLiteral.createCharString(p, getPos());
        }
    ]
    <AS>
    query = RichOrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateView(s.pos(), viewName, fieldList, query, replace, isTemporary, ifNotExists, comment, null);
    }
}

SqlDrop SqlDropView(Span s, boolean replace, boolean isTemporary) :
{
    SqlIdentifier viewName = null;
    boolean ifExists = false;
}
{
    <VIEW>

    ifExists = IfExistsOpt()

    viewName = CompoundIdentifier()
    {
        return new SqlDropView(s.pos(), viewName, ifExists, isTemporary);
    }
}

SqlAlterView SqlAlterView() :
{
  SqlParserPos startPos;
  SqlIdentifier viewName;
  SqlIdentifier newViewName;
  SqlNode newQuery;
}
{
  <ALTER> <VIEW> { startPos = getPos(); }
  viewName = CompoundIdentifier()
  (
      <RENAME> <TO>
      newViewName = CompoundIdentifier()
      {
        return new SqlAlterViewRename(startPos.plus(getPos()), viewName, newViewName);
      }
  |
      <AS>
      newQuery = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
      {
        return new SqlAlterViewAs(startPos.plus(getPos()), viewName, newQuery);
      }
  )
}

/**
* A sql type name extended basic data type, it has a counterpart basic
* sql type name but always represents as a special alias compared with the standard name.
*
* <p>For example:
*  1. STRING is synonym of VARCHAR(INT_MAX),
*  2. BYTES is synonym of VARBINARY(INT_MAX),
*  3. TIMESTAMP_LTZ(precision) is synonym of TIMESTAMP(precision) WITH LOCAL TIME ZONE.
*/
SqlTypeNameSpec ExtendedSqlBasicTypeName() :
{
    final SqlTypeName typeName;
    final String typeAlias;
    int precision = -1;
}
{
    (
        <STRING> {
            typeName = SqlTypeName.VARCHAR;
            typeAlias = token.image;
            precision = Integer.MAX_VALUE;
        }
    |
        <BYTES> {
            typeName = SqlTypeName.VARBINARY;
            typeAlias = token.image;
            precision = Integer.MAX_VALUE;
        }
    |
       <TIMESTAMP_LTZ>
       {
           typeAlias = token.image;
       }
       precision = PrecisionOpt()
       {
            typeName = SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
            return new SqlTimestampLtzTypeNameSpec(typeAlias, typeName, precision, getPos());
       }
    )
    {
        return new SqlAlienSystemTypeNameSpec(typeAlias, typeName, precision, getPos());
    }
}

/*
* Parses collection type name that does not belong to standard SQL, i.e. ARRAY&lt;INT NOT NULL&gt;.
*/
SqlTypeNameSpec CustomizedCollectionsTypeName() :
{
    final SqlTypeName collectionTypeName;
    final SqlTypeNameSpec elementTypeName;
    boolean elementNullable = true;
}
{
    (
        <ARRAY> {
            collectionTypeName = SqlTypeName.ARRAY;
        }
    |
        <MULTISET> {
            collectionTypeName = SqlTypeName.MULTISET;
        }
    )
    <LT>
    elementTypeName = TypeName()
    elementNullable = NullableOptDefaultTrue()
    <GT>
    {
        return new ExtendedSqlCollectionTypeNameSpec(
            elementTypeName,
            elementNullable,
            collectionTypeName,
            false,
            getPos());
    }
}

/**
* Parse a collection type name, the input element type name may
* also be a collection type. Different with #CollectionsTypeName,
* the element type can have a [ NULL | NOT NULL ] suffix, default is NULL(nullable).
*/
SqlTypeNameSpec ExtendedCollectionsTypeName(
        SqlTypeNameSpec elementTypeName,
        boolean elementNullable) :
{
    final SqlTypeName collectionTypeName;
}
{
    (
        <MULTISET> { collectionTypeName = SqlTypeName.MULTISET; }
    |
         <ARRAY> { collectionTypeName = SqlTypeName.ARRAY; }
    )
    {
        return new ExtendedSqlCollectionTypeNameSpec(
             elementTypeName,
             elementNullable,
             collectionTypeName,
             true,
             getPos());
    }
}

/** Parses a SQL map type, e.g. MAP&lt;INT NOT NULL, VARCHAR NULL&gt;. */
SqlTypeNameSpec SqlMapTypeName() :
{
    SqlDataTypeSpec keyType;
    SqlDataTypeSpec valType;
}
{
    <MAP>
    <LT>
    keyType = ExtendedDataType()
    <COMMA>
    valType = ExtendedDataType()
    <GT>
    {
        return new SqlMapTypeNameSpec(keyType, valType, getPos());
    }
}

/** Parses a SQL raw type such as {@code RAW('org.my.Class', 'sW3Djsds...')}. */
SqlTypeNameSpec SqlRawTypeName() :
{
    SqlNode className;
    SqlNode serializerString;
}
{
    <RAW>
    <LPAREN>
    className = StringLiteral()
    <COMMA>
    serializerString = StringLiteral()
    <RPAREN>
    {
        return new SqlRawTypeNameSpec(className, serializerString, getPos());
    }
}

/**
* Parse a "name1 type1 [ NULL | NOT NULL] [ comment ]
* [, name2 type2 [ NULL | NOT NULL] [ comment ] ]* ..." list.
* The comment and NULL syntax doest not belong to standard SQL.
*/
void ExtendedFieldNameTypeCommaList(
        List<SqlIdentifier> fieldNames,
        List<SqlDataTypeSpec> fieldTypes,
        List<SqlCharStringLiteral> comments) :
{
    SqlIdentifier fName;
    SqlDataTypeSpec fType;
    boolean nullable;
}
{
    [
        fName = SimpleIdentifier()
        fType = ExtendedDataType()
        {
            fieldNames.add(fName);
            fieldTypes.add(fType);
        }
        (
            <QUOTED_STRING> {
                String p = SqlParserUtil.parseString(token.image);
                comments.add(SqlLiteral.createCharString(p, getPos()));
            }
        |
            { comments.add(null); }
        )
    ]
    (
        <COMMA>
        fName = SimpleIdentifier()
        fType = ExtendedDataType()
        {
            fieldNames.add(fName);
            fieldTypes.add(fType);
        }
        (
            <QUOTED_STRING> {
                String p = SqlParserUtil.parseString(token.image);
                comments.add(SqlLiteral.createCharString(p, getPos()));
            }
        |
            { comments.add(null); }
        )
    )*
}

/**
* Parse Row type, we support both Row(name1 type1, name2 type2)
* and Row&lt;name1 type1, name2 type2&gt;.
* Every item type can have a suffix of `NULL` or `NOT NULL` to indicate if this type is nullable.
* i.e. Row(f0 int not null, f1 varchar null). Default is nullable.
*
* <p>The difference with {@link #SqlRowTypeName()}:
* <ul>
*   <li>Support comment syntax for every field</li>
*   <li>Field data type default is nullable</li>
*   <li>Support ROW type with empty fields, e.g. ROW()</li>
* </ul>
*/
SqlTypeNameSpec ExtendedSqlRowTypeName() :
{
    List<SqlIdentifier> fieldNames = new ArrayList<SqlIdentifier>();
    List<SqlDataTypeSpec> fieldTypes = new ArrayList<SqlDataTypeSpec>();
    List<SqlCharStringLiteral> comments = new ArrayList<SqlCharStringLiteral>();
    final boolean unparseAsStandard;
}
{
    <ROW>
    (
        <NE> { unparseAsStandard = false; }
    |
        <LT> ExtendedFieldNameTypeCommaList(fieldNames, fieldTypes, comments) <GT>
        { unparseAsStandard = false; }
    |
        <LPAREN> ExtendedFieldNameTypeCommaList(fieldNames, fieldTypes, comments) <RPAREN>
        { unparseAsStandard = true; }
    )
    {
        return new ExtendedSqlRowTypeNameSpec(
            getPos(),
            fieldNames,
            fieldTypes,
            comments,
            unparseAsStandard);
    }
}

/**
 * Those methods should not be used in SQL. They are good for parsing identifiers
 * in Table API. The difference between those identifiers and CompoundIdentifer is
 * that the Table API identifiers ignore any keywords. They are also strictly limited
 * to three part identifiers. The quoting still works the same way.
 */
SqlIdentifier TableApiIdentifier() :
{
    final List<String> nameList = new ArrayList<String>();
    final List<SqlParserPos> posList = new ArrayList<SqlParserPos>();
}
{
    TableApiIdentifierSegment(nameList, posList)
    (
        LOOKAHEAD(2)
        <DOT>
        TableApiIdentifierSegment(nameList, posList)
    )?
    (
        LOOKAHEAD(2)
        <DOT>
        TableApiIdentifierSegment(nameList, posList)
    )?
    <EOF>
    {
        SqlParserPos pos = SqlParserPos.sum(posList);
        return new SqlIdentifier(nameList, null, pos, posList);
    }
}

void TableApiIdentifierSegment(List<String> names, List<SqlParserPos> positions) :
{
    final String id;
    char unicodeEscapeChar = BACKSLASH;
    final SqlParserPos pos;
    final Span span;
}
{
    (
        <QUOTED_IDENTIFIER> {
            id = SqlParserUtil.strip(getToken(0).image, DQ, DQ, DQDQ,
                quotedCasing);
            pos = getPos().withQuoting(true);
        }
    |
        <BACK_QUOTED_IDENTIFIER> {
            id = SqlParserUtil.strip(getToken(0).image, "`", "`", "``",
                quotedCasing);
            pos = getPos().withQuoting(true);
        }
    |
        <BRACKET_QUOTED_IDENTIFIER> {
            id = SqlParserUtil.strip(getToken(0).image, "[", "]", "]]",
                quotedCasing);
            pos = getPos().withQuoting(true);
        }
    |
        <UNICODE_QUOTED_IDENTIFIER> {
            span = span();
            String image = getToken(0).image;
            image = image.substring(image.indexOf('"'));
            image = SqlParserUtil.strip(image, DQ, DQ, DQDQ, quotedCasing);
        }
        [
            <UESCAPE> <QUOTED_STRING> {
                String s = SqlParserUtil.parseString(token.image);
                unicodeEscapeChar = SqlParserUtil.checkUnicodeEscapeChar(s);
            }
        ]
        {
            pos = span.end(this).withQuoting(true);
            SqlLiteral lit = SqlLiteral.createCharString(image, "UTF16", pos);
            lit = lit.unescapeUnicode(unicodeEscapeChar);
            id = lit.toValue();
        }
    |
        {

            id = getNextToken().image;
            pos = getPos();
        }
    )
    {
        if (id.length() > this.identifierMaxLength) {
            throw SqlUtil.newContextException(pos,
                RESOURCE.identifierTooLong(id, this.identifierMaxLength));
        }
        names.add(id);
        if (positions != null) {
            positions.add(pos);
        }
    }
}

SqlCreate SqlCreateExtended(Span s, boolean replace) :
{
    final SqlCreate create;
    boolean isTemporary = false;
}
{
    [
        <TEMPORARY> { isTemporary = true; }
    ]
    (
        create = SqlCreateCatalog(s, replace)
        |
        create = SqlCreateTable(s, replace, isTemporary)
        |
        create = SqlCreateView(s, replace, isTemporary)
        |
        create = SqlCreateDatabase(s, replace)
        |
        create = SqlCreateFunction(s, replace, isTemporary)
    )
    {
        return create;
    }
}

SqlDrop SqlDropExtended(Span s, boolean replace) :
{
    final SqlDrop drop;
    boolean isTemporary = false;
}
{
    [
        <TEMPORARY> { isTemporary = true; }
    ]
    (
        drop = SqlDropCatalog(s, replace)
        |
        drop = SqlDropTable(s, replace, isTemporary)
        |
        drop = SqlDropView(s, replace, isTemporary)
        |
        drop = SqlDropDatabase(s, replace)
        |
        drop = SqlDropFunction(s, replace, isTemporary)
    )
    {
        return drop;
    }
}

/**
* Parses a load module statement.
* LOAD MODULE module_name [WITH (property_name=property_value, ...)];
*/
SqlLoadModule SqlLoadModule() :
{
    SqlParserPos startPos;
    SqlIdentifier moduleName;
    SqlNodeList propertyList = SqlNodeList.EMPTY;
}
{
    <LOAD> <MODULE> { startPos = getPos(); }
    moduleName = SimpleIdentifier()
    [
        <WITH>
        propertyList = TableProperties()
    ]
    {
        return new SqlLoadModule(startPos.plus(getPos()),
            moduleName,
            propertyList);
    }
}

/**
* Parses an unload module statement.
* UNLOAD MODULE module_name;
*/
SqlUnloadModule SqlUnloadModule() :
{
    SqlParserPos startPos;
    SqlIdentifier moduleName;
}
{
    <UNLOAD> <MODULE> { startPos = getPos(); }
    moduleName = SimpleIdentifier()
    {
        return new SqlUnloadModule(startPos.plus(getPos()), moduleName);
    }
}

/**
* Parses an use modules statement.
* USE MODULES module_name1 [, module_name2, ...];
*/
SqlUseModules SqlUseModules() :
{
    final Span s;
    SqlIdentifier moduleName;
    final List<SqlIdentifier> moduleNames = new ArrayList<SqlIdentifier>();
}
{
    <USE> <MODULES> { s = span(); }
    moduleName = SimpleIdentifier()
    {
        moduleNames.add(moduleName);
    }
    [
        (
            <COMMA>
            moduleName = SimpleIdentifier()
            {
                moduleNames.add(moduleName);
            }
        )+
    ]
    {
        return new SqlUseModules(s.end(this), moduleNames);
    }
}

/**
* Parses a show modules statement.
* SHOW [FULL] MODULES;
*/
SqlShowModules SqlShowModules() :
{
    SqlParserPos startPos;
    boolean requireFull = false;
}
{
    <SHOW> { startPos = getPos(); }
    [
      <FULL> { requireFull = true; }
    ]
    <MODULES>
    {
        return new SqlShowModules(startPos.plus(getPos()), requireFull);
    }
}

/**
* Parse a start statement set statement.
* BEGIN STATEMENT SET;
*/
SqlBeginStatementSet SqlBeginStatementSet() :
{
}
{
    <BEGIN> <STATEMENT> <SET>
    {
        return new SqlBeginStatementSet(getPos());
    }
}

/**
* Parse a end statement set statement.
* END;
*/
SqlEndStatementSet SqlEndStatementSet() :
{
}
{
    <END>
    {
        return new SqlEndStatementSet(getPos());
    }
}

/**
* Parse a statement set.
*
* STATEMENT SET BEGIN (RichSqlInsert();)+ END
*/
SqlNode SqlStatementSet() :
{
    SqlParserPos startPos;
    SqlNode insert;
    List<RichSqlInsert> inserts = new ArrayList<RichSqlInsert>();
}
{
    <STATEMENT>{ startPos = getPos(); } <SET> <BEGIN>
    (
        insert = RichSqlInsert()
        <SEMICOLON>
        {
            inserts.add((RichSqlInsert) insert);
        }
    )+
    <END>
    {
        return new SqlStatementSet(inserts, startPos);
    }
}

/**
* Parses an explain module statement.
*/
SqlNode SqlRichExplain() :
{
    SqlNode stmt;
    Set<String> explainDetails = new HashSet<String>();
}
{
    (
    LOOKAHEAD(3) <EXPLAIN> <PLAN> <FOR>
    |
    LOOKAHEAD(2) <EXPLAIN> ParseExplainDetail(explainDetails) ( <COMMA> ParseExplainDetail(explainDetails) )*
    |
    <EXPLAIN>
    )
    (
        stmt = SqlStatementSet()
        |
        stmt = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
        |
        stmt = RichSqlInsert()
    )
    {
        return new SqlRichExplain(getPos(), stmt, explainDetails);
    }
}

/**
* Parses an execute statement.
*/
SqlNode SqlExecute() :
{
    SqlParserPos startPos;
    SqlNode stmt;
}
{
    <EXECUTE>{ startPos = getPos(); }
    (
        stmt = SqlStatementSet()
        |
        stmt = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
        |
        stmt = RichSqlInsert()
    )
    {
        return new SqlExecute(stmt, startPos);
    }
}

/**
* Parses an execute plan statement.
*/
SqlNode SqlExecutePlan() :
{
    SqlNode filePath;
}
{
    <EXECUTE> <PLAN>

    filePath = StringLiteral()

    {
        return new SqlExecutePlan(getPos(), filePath);
    }
}

/**
* Parses a compile plan statement.
*/
SqlNode SqlCompileAndExecutePlan() :
{
    SqlParserPos startPos;
    SqlNode filePath;
    SqlNode operand;
}
{
    <COMPILE> <AND> <EXECUTE> <PLAN> { startPos = getPos(); }

    filePath = StringLiteral()

    <FOR>

    (
        operand = SqlStatementSet()
    |
        operand = RichSqlInsert()
    )

    {
        return new SqlCompileAndExecutePlan(startPos, filePath, operand);
    }
}

/**
* Parses a compile plan statement.
*/
SqlNode SqlCompilePlan() :
{
    SqlParserPos startPos;
    SqlNode filePath;
    boolean ifNotExists;
    SqlNode operand;
}
{
    <COMPILE> <PLAN> { startPos = getPos(); }

    filePath = StringLiteral()

    ifNotExists = IfNotExistsOpt()

    <FOR>

    (
        operand = SqlStatementSet()
    |
        operand = RichSqlInsert()
    )

    {
        return new SqlCompilePlan(startPos, filePath, ifNotExists, operand);
    }
}

void ParseExplainDetail(Set<String> explainDetails):
{
}
{
    (
        <ESTIMATED_COST> 
        | 
        <CHANGELOG_MODE> 
        | 
        <JSON_EXECUTION_PLAN>
    ) 
    {
        if (explainDetails.contains(token.image.toUpperCase())) {
            throw SqlUtil.newContextException(
                getPos(),
                ParserResource.RESOURCE.explainDetailIsDuplicate());
        } else {
            explainDetails.add(token.image.toUpperCase());
        }
    }
}

/**
* Parses an ADD JAR statement.
*/
SqlAddJar SqlAddJar() :
{
    SqlCharStringLiteral jarPath;
}
{
    <ADD> <JAR> <QUOTED_STRING>
    {
        String path = SqlParserUtil.parseString(token.image);
        jarPath = SqlLiteral.createCharString(path, getPos());
    }
    {
        return new SqlAddJar(getPos(), jarPath);
    }
}

/**
* Parses a remove jar statement.
* REMOVE JAR jar_path;
*/
SqlRemoveJar SqlRemoveJar() :
{
    SqlCharStringLiteral jarPath;
}
{
    <REMOVE> <JAR> <QUOTED_STRING>
    {
        String path = SqlParserUtil.parseString(token.image);
        jarPath = SqlLiteral.createCharString(path, getPos());
    }
    {
        return new SqlRemoveJar(getPos(), jarPath);
    }
}

/**
* Parses a show jars statement.
* SHOW JARS;
*/
SqlShowJars SqlShowJars() :
{
}
{
    <SHOW> <JARS>
    {
        return new SqlShowJars(getPos());
    }
}

/*
* Parses a SET statement:
* SET ['key' = 'value'];
*/
SqlNode SqlSet() :
{
    Span s;
    SqlNode key = null;
    SqlNode value = null;
}
{
    <SET> { s = span(); }
    [
        key = StringLiteral()
        <EQ>
        value = StringLiteral()
    ]
    {
        if (key == null && value == null) {
            return new SqlSet(s.end(this));
        } else {
            return new SqlSet(s.end(this), key, value);
        }
    }
}

/**
* Parses a RESET statement:
* RESET ['key'];
*/
SqlNode SqlReset() :
{
    Span span;
    SqlNode key = null;
}
{
    <RESET> { span = span(); }
    [
        key = StringLiteral()
    ]
    {
        return new SqlReset(span.end(this), key);
    }
}


/** Parses a TRY_CAST invocation. */
SqlNode TryCastFunctionCall() :
{
    final Span s;
    final SqlOperator operator;
    List<SqlNode> args = null;
    SqlNode e = null;
}
{
    <TRY_CAST> {
        s = span();
        operator = new SqlUnresolvedTryCastFunction(s.pos());
    }
    <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) { args = startList(e); }
    <AS>
    (
        e = DataType() { args.add(e); }
    |
        <INTERVAL> e = IntervalQualifier() { args.add(e); }
    )
    <RPAREN>
    {
        return operator.createCall(s.end(this), args);
    }
}

/**
* Parses a partition key/value,
* e.g. p or p = '10'.
*/
SqlPartitionSpecProperty PartitionSpecProperty():
{
    final SqlParserPos pos;
    final SqlIdentifier key;
    SqlNode value = null;
}
{
    key = SimpleIdentifier() { pos = getPos(); }
    [
        LOOKAHEAD(1)
        <EQ> value = Literal()
    ]
    {
        return new SqlPartitionSpecProperty(key, value, pos);
    }
}

/**
* Parses a partition specifications statement,
* e.g. ANALYZE TABLE tbl1 partition(col1='val1', col2='val2') xxx
* or
* ANALYZE TABLE tbl1 partition(col1, col2) xxx.
* or
* ANALYZE TABLE tbl1 partition(col1='val1', col2) xxx.
*/
void ExtendedPartitionSpecCommaList(SqlNodeList list) :
{
    SqlPartitionSpecProperty property;
}
{
    <LPAREN>
    property = PartitionSpecProperty()
    {
       list.add(property);
    }
    (
        <COMMA> property = PartitionSpecProperty()
        {
            list.add(property);
        }
    )*
    <RPAREN>
}

/** Parses a comma-separated list of simple identifiers with position. */
SqlNodeList SimpleIdentifierCommaListWithPosition() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    { s = span(); }
    SimpleIdentifierCommaList(list) {
        return new SqlNodeList(list, s.end(this));
    }
}

/** Parses an ANALYZE TABLE statement. */
SqlNode SqlAnalyzeTable():
{
       final Span s;
       final SqlIdentifier tableName;
       SqlNodeList partitionSpec = SqlNodeList.EMPTY;
       SqlNodeList columns = SqlNodeList.EMPTY;
       boolean allColumns = false;
}
{
    <ANALYZE> <TABLE> { s = span(); }
    tableName = CompoundIdentifier()
    [
        <PARTITION> {
            partitionSpec = new SqlNodeList(getPos());
            ExtendedPartitionSpecCommaList(partitionSpec);
        }
    ]

    <COMPUTE> <STATISTICS> [ <FOR>
        (
           <COLUMNS> { columns = SimpleIdentifierCommaListWithPosition(); }
        |
           <ALL> <COLUMNS> { allColumns = true; }
        )
    ]

    {
        return new SqlAnalyzeTable(s.end(this), tableName, partitionSpec, columns, allColumns);
    }
}


/**
 * Parses either a row expression or a query expression with an optional
 * ORDER BY.
 *
 * <p>Postgres syntax for limit:
 *
 * <blockquote><pre>
 *    [ LIMIT { count | ALL } ]
 *    [ OFFSET start ]</pre>
 * </blockquote>
 *
 * <p>MySQL syntax for limit:
 *
 * <blockquote><pre>
 *    [ LIMIT { count | start, count } ]</pre>
 * </blockquote>
 *
 * <p>SQL:2008 syntax for limit:
 *
 * <blockquote><pre>
 *    [ OFFSET start { ROW | ROWS } ]
 *    [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY ]</pre>
 * </blockquote>
 */
SqlNode RichOrderedQueryOrExpr(ExprContext exprContext) :
{
    SqlNode e;
    SqlNodeList orderBy = null;
    SqlNode start = null;
    SqlNode count = null;
}
{
    (
        e = RichQueryOrExpr(exprContext)
    )
    [
        // use the syntactic type of the expression we just parsed
        // to decide whether ORDER BY makes sense
        orderBy = OrderBy(e.isA(SqlKind.QUERY))
    ]
    [
        // Postgres-style syntax. "LIMIT ... OFFSET ..."
        <LIMIT>
        (
            // MySQL-style syntax. "LIMIT start, count"
            LOOKAHEAD(2)
            start = UnsignedNumericLiteralOrParam()
            <COMMA> count = UnsignedNumericLiteralOrParam() {
                if (!this.conformance.isLimitStartCountAllowed()) {
                    throw SqlUtil.newContextException(getPos(), RESOURCE.limitStartCountNotAllowed());
                }
            }
        |
            count = UnsignedNumericLiteralOrParam()
        |
            <ALL>
        )
    ]
    [
        // ROW or ROWS is required in SQL:2008 but we make it optional
        // because it is not present in Postgres-style syntax.
        // If you specify both LIMIT start and OFFSET, OFFSET wins.
        <OFFSET> start = UnsignedNumericLiteralOrParam() [ <ROW> | <ROWS> ]
    ]
    [
        // SQL:2008-style syntax. "OFFSET ... FETCH ...".
        // If you specify both LIMIT and FETCH, FETCH wins.
        <FETCH> ( <FIRST> | <NEXT> ) count = UnsignedNumericLiteralOrParam()
        ( <ROW> | <ROWS> ) <ONLY>
    ]
    {
        if (orderBy != null || start != null || count != null) {
            if (orderBy == null) {
                orderBy = SqlNodeList.EMPTY;
            }
            e = new SqlOrderBy(getPos(), e, orderBy, start, count);

        }
        return e;
    }
}
/**
 * Parses either a row expression or a query expression without ORDER BY.
 */
SqlNode RichQueryOrExpr(ExprContext exprContext) :
{
    SqlNodeList withList = null;
    SqlNode e;
    SqlOperator op;
    SqlParserPos pos;
    SqlParserPos withPos;
    List<Object> list;
}
{
    [
        withList = WithList()
    ]
    e = RichLeafQueryOrExpr(exprContext) {
        list = startList(e);
    }
    (
        {
            if (!e.isA(SqlKind.QUERY)) {
                // whoops, expression we just parsed wasn't a query,
                // but we're about to see something like UNION, so
                // force an exception retroactively
                checkNonQueryExpression(ExprContext.ACCEPT_QUERY);
            }
        }
        op = BinaryQueryOperator() {
            // ensure a query is legal in this context
            pos = getPos();
            checkQueryExpression(exprContext);

        }
        e = RichLeafQueryOrExpr(ExprContext.ACCEPT_QUERY) {
            list.add(new SqlParserUtil.ToTreeListItem(op, pos));
            list.add(e);
        }
    )*
    {
        e = SqlParserUtil.toTree(list);
        if (withList != null) {
            e = new SqlWith(withList.getParserPosition(), withList, e);
        }
        return e;
    }
}

/**
 * Parses either a row expression, a leaf query expression, or
 * a parenthesized expression of any kind.
 */
SqlNode RichLeafQueryOrExpr(ExprContext exprContext) :
{
    SqlNode e;
}
{
    e = Expression(exprContext) { return e; }
|
    e = RichLeafQuery(exprContext) { return e; }
}

/**
 * Parses a leaf in a query expression (SELECT, VALUES or TABLE).
 */
SqlNode RichLeafQuery(ExprContext exprContext) :
{
    SqlNode e;
}
{
    {
        // ensure a query is legal in this context
        checkQueryExpression(exprContext);
    }
    e = RichSqlSelect() { return e; }
|
    e = TableConstructor() { return e; }
|
    e = ExplicitTable(getPos()) { return e; }
}
/**
 * Parses a leaf SELECT expression without ORDER BY.
 */
SqlSelect RichSqlSelect() :
{
    final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    final SqlNodeList keywordList;
    List<SqlNode> selectList;
    final SqlNode fromClause;
    final SqlNode where;
    final SqlNodeList groupBy;
    final SqlNode having;
    final SqlNodeList windowDecls;
    final List<SqlNode> hints = new ArrayList<SqlNode>();
    final Span s;
}
{
    <SELECT>
    {
        s = span();
    }
    [
        <HINT_BEG>
        CommaSeparatedSqlHints(hints)
        <COMMENT_END>
    ]
    SqlSelectKeywords(keywords)
    (
        <STREAM> {
            keywords.add(SqlSelectKeyword.STREAM.symbol(getPos()));
        }
    )?
    (
        <DISTINCT> {
            keywords.add(SqlSelectKeyword.DISTINCT.symbol(getPos()));
        }
    |   <ALL> {
            keywords.add(SqlSelectKeyword.ALL.symbol(getPos()));
        }
    )?
    {
        keywordList = new SqlNodeList(keywords, s.addAll(keywords).pos());
    }
    selectList = SelectList()
    (
        <FROM> fromClause = RichFromClause()
        where = WhereOpt()
        groupBy = GroupByOpt()
        having = HavingOpt()
        windowDecls = WindowOpt()
    |
        E() {
            fromClause = null;
            where = null;
            groupBy = null;
            having = null;
            windowDecls = null;
        }
    )
    {
        return new SqlSelect(s.end(this), keywordList,
            new SqlNodeList(selectList, Span.of(selectList).pos()),
            fromClause, where, groupBy, having, windowDecls, null, null, null,
            new SqlNodeList(hints, getPos()));
    }
}

// TODO jvs 15-Nov-2003:  SQL standard allows parentheses in the FROM list for
// building up non-linear join trees (e.g. OUTER JOIN two tables, and then INNER
// JOIN the result).  Also note that aliases on parenthesized FROM expressions
// "hide" all table names inside the parentheses (without aliases, they're
// visible).
//
// We allow CROSS JOIN to have a join condition, even though that is not valid
// SQL; the validator will catch it.
/**
 * Parses the FROM clause for a SELECT.
 *
 * <p>FROM is mandatory in standard SQL, optional in dialects such as MySQL,
 * PostgreSQL. The parser allows SELECT without FROM, but the validator fails
 * if conformance is, say, STRICT_2003.
 */
SqlNode RichFromClause() :
{
    SqlNode e, e2, condition;
    SqlLiteral natural, joinType, joinConditionType;
    SqlNodeList list;
    SqlParserPos pos;
}
{
    e = RichTableRef()
    (
        LOOKAHEAD(2)
        (
            // Decide whether to read a JOIN clause or a comma, or to quit having
            // seen a single entry FROM clause like 'FROM emps'. See comments
            // elsewhere regarding <COMMA> lookahead.
            //
            // And LOOKAHEAD(3) is needed here rather than a LOOKAHEAD(2). Because currently JavaCC
            // calculates minimum lookahead count incorrectly for choice that contains zero size
            // child. For instance, with the generated code, "LOOKAHEAD(2, Natural(), JoinType())"
            // returns true immediately if it sees a single "<CROSS>" token. Where we expect
            // the lookahead succeeds after "<CROSS> <APPLY>".
            //
            // For more information about the issue, see https://github.com/javacc/javacc/issues/86
            LOOKAHEAD(3)
            natural = Natural()
            joinType = JoinType()
            e2 = RichTableRef()
            (
                <ON> {
                    joinConditionType = JoinConditionType.ON.symbol(getPos());
                }
                condition = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                    e = new SqlJoin(joinType.getParserPosition(),
                        e,
                        natural,
                        joinType,
                        e2,
                        joinConditionType,
                        condition);
                }
            |
                <USING> {
                    joinConditionType = JoinConditionType.USING.symbol(getPos());
                }
                list = ParenthesizedSimpleIdentifierList() {
                    e = new SqlJoin(joinType.getParserPosition(),
                        e,
                        natural,
                        joinType,
                        e2,
                        joinConditionType,
                        new SqlNodeList(list.getList(), Span.of(joinConditionType).end(this)));
                }
            |
                {
                    e = new SqlJoin(joinType.getParserPosition(),
                        e,
                        natural,
                        joinType,
                        e2,
                        JoinConditionType.NONE.symbol(joinType.getParserPosition()),
                        null);
                }
            )
        |
            // NOTE jvs 6-Feb-2004:  See comments at top of file for why
            // hint is necessary here.  I had to use this special semantic
            // lookahead form to get JavaCC to shut up, which makes
            // me even more uneasy.
            //LOOKAHEAD({true})
            <COMMA> { joinType = JoinType.COMMA.symbol(getPos()); }
            e2 = RichTableRef() {
                e = new SqlJoin(joinType.getParserPosition(),
                    e,
                    SqlLiteral.createBoolean(false, joinType.getParserPosition()),
                    joinType,
                    e2,
                    JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                    null);
            }
        |
            <CROSS> { joinType = JoinType.CROSS.symbol(getPos()); } <APPLY>
            e2 = RichTableRef2(true) {
                if (!this.conformance.isApplyAllowed()) {
                    throw SqlUtil.newContextException(getPos(), RESOURCE.applyNotAllowed());
                }
                e = new SqlJoin(joinType.getParserPosition(),
                    e,
                    SqlLiteral.createBoolean(false, joinType.getParserPosition()),
                    joinType,
                    e2,
                    JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                    null);
            }
        |
            <OUTER> { joinType = JoinType.LEFT.symbol(getPos()); } <APPLY>
            e2 = RichTableRef2(true) {
                if (!this.conformance.isApplyAllowed()) {
                    throw SqlUtil.newContextException(getPos(), RESOURCE.applyNotAllowed());
                }
                e = new SqlJoin(joinType.getParserPosition(),
                    e,
                    SqlLiteral.createBoolean(false, joinType.getParserPosition()),
                    joinType,
                    e2,
                    JoinConditionType.ON.symbol(SqlParserPos.ZERO),
                    SqlLiteral.createBoolean(true, joinType.getParserPosition()));
            }
        )
    )*
    {
        return e;
    }
}
/**
 * Parses a parenthesized query or single row expression.
 */
SqlNode RichParenthesizedExpression(ExprContext exprContext) :
{
    SqlNode e;
}
{
    <LPAREN>
    {
        // we've now seen left paren, so queries inside should
        // be allowed as sub-queries
        switch (exprContext) {
        case ACCEPT_SUB_QUERY:
            exprContext = ExprContext.ACCEPT_NONCURSOR;
            break;
        case ACCEPT_CURSOR:
            exprContext = ExprContext.ACCEPT_ALL;
            break;
        }
    }
    e = RichOrderedQueryOrExpr(exprContext)
    <RPAREN>
    {
        return e;
    }
}
/**
 * Parses a table reference in a FROM clause, not lateral unless LATERAL
 * is explicitly specified.
 */
SqlNode RichTableRef() :
{
    final SqlNode e;
}
{
    e = RichTableRef2(false) { return e; }
}

/**
 * Parses a table reference in a FROM clause.
 */
SqlNode RichTableRef2(boolean lateral) :
{
    SqlNode tableRef;
    final SqlNode over;
    final SqlNode snapshot;
    final SqlNode match;
    SqlNodeList extendList = null;
    final SqlIdentifier alias;
    final Span s, s2;
    SqlNodeList args;
    SqlNode sample;
    boolean isBernoulli;
    SqlNumericLiteral samplePercentage;
    boolean isRepeatable = false;
    int repeatableSeed = 0;
    SqlNodeList columnAliasList = null;
    SqlUnnestOperator unnestOp = SqlStdOperatorTable.UNNEST;
}
{
    (
        LOOKAHEAD(2)
        tableRef = TableRefWithHintsOpt()
        [
            [ <EXTEND> ]
            extendList = ExtendList() {
                tableRef = extend(tableRef, extendList);
            }
        ]
        over = TableOverOpt() {
            if (over != null) {
                tableRef = SqlStdOperatorTable.OVER.createCall(
                    getPos(), tableRef, over);
            }
        }
        [
            tableRef = Snapshot(tableRef)
        ]
        [
            tableRef = RichMatchRecognize(tableRef)
        ]
    |
        LOOKAHEAD(2)
        [ <LATERAL> { lateral = true; } ]
        tableRef = RichParenthesizedExpression(ExprContext.ACCEPT_QUERY)
        over = TableOverOpt()
        {
            if (over != null) {
                tableRef = SqlStdOperatorTable.OVER.createCall(
                    getPos(), tableRef, over);
            }
            if (lateral) {
                tableRef = SqlStdOperatorTable.LATERAL.createCall(
                    getPos(), tableRef);
            }
        }
        [
            tableRef = RichMatchRecognize(tableRef)
        ]
    |
        <UNNEST> { s = span(); }
        args = ParenthesizedQueryOrCommaList(ExprContext.ACCEPT_SUB_QUERY)
        [
            <WITH> <ORDINALITY> {
                unnestOp = SqlStdOperatorTable.UNNEST_WITH_ORDINALITY;
            }
        ]
        {
            tableRef = unnestOp.createCall(s.end(this), args.toArray());
        }
    |
        [<LATERAL> { lateral = true; } ]
        <TABLE> { s = span(); } <LPAREN>
        tableRef = TableFunctionCall(s.pos())
        <RPAREN>
        {
            if (lateral) {
                tableRef = SqlStdOperatorTable.LATERAL.createCall(
                    s.end(this), tableRef);
            }
        }
    |
        tableRef = ExtendedTableRef()
    )
    [
        tableRef = Pivot(tableRef)
    ]
    [
        [ <AS> ] alias = SimpleIdentifier()
        [ columnAliasList = ParenthesizedSimpleIdentifierList() ]
        {
            if (columnAliasList == null) {
                tableRef = SqlStdOperatorTable.AS.createCall(
                    Span.of(tableRef).end(this), tableRef, alias);
            } else {
                List<SqlNode> idList = new ArrayList<SqlNode>();
                idList.add(tableRef);
                idList.add(alias);
                idList.addAll(columnAliasList.getList());
                tableRef = SqlStdOperatorTable.AS.createCall(
                    Span.of(tableRef).end(this), idList);
            }
        }
    ]
    [
        <TABLESAMPLE> { s2 = span(); }
        (
            <SUBSTITUTE> <LPAREN> sample = StringLiteral() <RPAREN>
            {
                String sampleName =
                    SqlLiteral.unchain(sample).getValueAs(String.class);
                SqlSampleSpec sampleSpec = SqlSampleSpec.createNamed(sampleName);
                final SqlLiteral sampleLiteral =
                    SqlLiteral.createSample(sampleSpec, s2.end(this));
                tableRef = SqlStdOperatorTable.TABLESAMPLE.createCall(
                    s2.add(tableRef).end(this), tableRef, sampleLiteral);
            }
        |
            (
                <BERNOULLI>
                {
                    isBernoulli = true;
                }
            |
                <SYSTEM>
                {
                    isBernoulli = false;
                }
            )
            <LPAREN> samplePercentage = UnsignedNumericLiteral() <RPAREN>
            [
                <REPEATABLE> <LPAREN> repeatableSeed = IntLiteral() <RPAREN>
                {
                    isRepeatable = true;
                }
            ]
            {
                final BigDecimal ONE_HUNDRED = BigDecimal.valueOf(100L);
                BigDecimal rate = samplePercentage.bigDecimalValue();
                if (rate.compareTo(BigDecimal.ZERO) < 0
                    || rate.compareTo(ONE_HUNDRED) > 0)
                {
                    throw SqlUtil.newContextException(getPos(), RESOURCE.invalidSampleSize());
                }

                // Treat TABLESAMPLE(0) and TABLESAMPLE(100) as no table
                // sampling at all.  Not strictly correct: TABLESAMPLE(0)
                // should produce no output, but it simplifies implementation
                // to know that some amount of sampling will occur.
                // In practice values less than ~1E-43% are treated as 0.0 and
                // values greater than ~99.999997% are treated as 1.0
                float fRate = rate.divide(ONE_HUNDRED).floatValue();
                if (fRate > 0.0f && fRate < 1.0f) {
                    SqlSampleSpec tableSampleSpec =
                    isRepeatable
                        ? SqlSampleSpec.createTableSample(
                            isBernoulli, fRate, repeatableSeed)
                        : SqlSampleSpec.createTableSample(isBernoulli, fRate);

                    SqlLiteral tableSampleLiteral =
                        SqlLiteral.createSample(tableSampleSpec, s2.end(this));
                    tableRef = SqlStdOperatorTable.TABLESAMPLE.createCall(
                        s2.end(this), tableRef, tableSampleLiteral);
                }
            }
        )
    ]
    {
        return tableRef;
    }
}

/**
 * Parses a MATCH_RECOGNIZE clause following a table expression.
 */
SqlMatchRecognize RichMatchRecognize(SqlNode tableRef) :
{
    final Span s, s0, s1, s2;
    SqlNodeList measureList = SqlNodeList.EMPTY;
    SqlNodeList partitionList = SqlNodeList.EMPTY;
    SqlNodeList orderList = SqlNodeList.EMPTY;
    SqlNode pattern;
    SqlLiteral interval;
    SqlNodeList patternDefList;
    final SqlNode after;
    SqlParserPos pos;
    final SqlNode var;
    final SqlLiteral rowsPerMatch;
    SqlNodeList subsetList = SqlNodeList.EMPTY;
    SqlLiteral isStrictStarts = SqlLiteral.createBoolean(false, getPos());
    SqlLiteral isStrictEnds = SqlLiteral.createBoolean(false, getPos());
}
{
    <MATCH_RECOGNIZE> { s = span(); } <LPAREN>
    [
        <PARTITION> { s2 = span(); } <BY>
        partitionList = ExpressionCommaList(s2, ExprContext.ACCEPT_NON_QUERY)
    ]
    [
        orderList = OrderBy(true)
    ]
    [
        <MEASURES>
        measureList = MeasureColumnCommaList(span())
    ]
    (
        <ONE> { s0 = span(); } <ROW> <PER> <MATCH> {
            rowsPerMatch = SqlMatchRecognize.RowsPerMatchOption.ONE_ROW.symbol(s0.end(this));
        }
    |
        <ALL> { s0 = span(); } <ROWS> <PER> <MATCH> {
            rowsPerMatch = SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS.symbol(s0.end(this));
        }
    |
        {
            rowsPerMatch = null;
        }
    )
    (
        <AFTER> { s1 = span(); } <MATCH> <SKIP_>
        (
            <TO>
            (
                LOOKAHEAD(2)
                <NEXT> <ROW> {
                    after = SqlMatchRecognize.AfterOption.SKIP_TO_NEXT_ROW
                        .symbol(s1.end(this));
                }
            |
                LOOKAHEAD(2)
                <FIRST> var = SimpleIdentifier() {
                    after = SqlMatchRecognize.SKIP_TO_FIRST.createCall(
                        s1.end(var), var);
                }
            |
                // This "LOOKAHEAD({true})" is a workaround for Babel.
                // Because of babel parser uses option "LOOKAHEAD=2" globally,
                // JavaCC generates something like "LOOKAHEAD(2, [<LAST>] SimpleIdentifier())"
                // here. But the correct LOOKAHEAD should be
                // "LOOKAHEAD(2, [ LOOKAHEAD(2, <LAST> SimpleIdentifier()) <LAST> ]
                // SimpleIdentifier())" which have the syntactic lookahead for <LAST> considered.
                //
                // Overall LOOKAHEAD({true}) is even better as this is the last branch in the
                // choice.
                LOOKAHEAD({true})
                [ LOOKAHEAD(2, <LAST> SimpleIdentifier()) <LAST> ] var = SimpleIdentifier() {
                    after = SqlMatchRecognize.SKIP_TO_LAST.createCall(
                        s1.end(var), var);
                }
            )
        |
            <PAST> <LAST> <ROW> {
                 after = SqlMatchRecognize.AfterOption.SKIP_PAST_LAST_ROW
                     .symbol(s1.end(this));
            }
        )
    |
        { after = null; }
    )
    <PATTERN>
    <LPAREN>
    (
        <CARET> { isStrictStarts = SqlLiteral.createBoolean(true, getPos()); }
    |
        { isStrictStarts = SqlLiteral.createBoolean(false, getPos()); }
    )
    pattern = RichPatternExpression()
    (
        <DOLLAR> { isStrictEnds = SqlLiteral.createBoolean(true, getPos()); }
    |
        { isStrictEnds = SqlLiteral.createBoolean(false, getPos()); }
    )
    <RPAREN>
    (
        <WITHIN> interval = IntervalLiteral()
    |
        { interval = null; }
    )
    [
        <SUBSET>
        subsetList = SubsetDefinitionCommaList(span())
    ]
    <DEFINE>
    patternDefList = PatternDefinitionCommaList(span())
    <RPAREN> {
        return new SqlMatchRecognize(s.end(this), tableRef,
            pattern, isStrictStarts, isStrictEnds, patternDefList, measureList,
            after, subsetList, rowsPerMatch, partitionList, orderList, interval);
    }
}

SqlNode RichPatternExpression() :
{
    SqlNode left;
    SqlNode right;
}
{
    left = RichPatternTerm()
    (
        <VERTICAL_BAR>
        right = RichPatternTerm() {
            left = SqlStdOperatorTable.PATTERN_ALTER.createCall(
                Span.of(left).end(right), left, right);
        }
    )*
    {
        return left;
    }
}

SqlNode RichPatternTerm() :
{
    SqlNode left;
    SqlNode right;
}
{
    left = RichPatternFactor()
    (
        right = RichPatternFactor() {
            left = SqlStdOperatorTable.PATTERN_CONCAT.createCall(
                Span.of(left).end(right), left, right);
        }
    )*
    {
        return left;
    }
}

SqlNode RichPatternFactor() :
{
    SqlNode e;
    SqlNode extra;
    SqlLiteral startNum = null;
    SqlLiteral endNum = null;
    SqlLiteral reluctant = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
}
{
    e = RichPatternPrimary()
    [
        LOOKAHEAD(1)
        (
            <STAR> {
                startNum = SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO);
                endNum = SqlLiteral.createExactNumeric("-1", SqlParserPos.ZERO);
            }
        |
            <PLUS> {
                startNum = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
                endNum = SqlLiteral.createExactNumeric("-1", SqlParserPos.ZERO);
            }
        |
            <HOOK> {
                startNum = SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO);
                endNum = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
            }
        |
            <LBRACE>
            (
                startNum = UnsignedNumericLiteral() { endNum = startNum; }
                [
                    <COMMA> {
                        endNum = SqlLiteral.createExactNumeric("-1", SqlParserPos.ZERO);
                    }
                    [
                        endNum = UnsignedNumericLiteral()
                    ]
                ]
                <RBRACE>
            |
                {
                    startNum = SqlLiteral.createExactNumeric("-1", SqlParserPos.ZERO);
                }
                <COMMA>
                endNum = UnsignedNumericLiteral()
                <RBRACE>
            |
                <MINUS> extra = RichPatternExpression() <MINUS> <RBRACE> {
                    extra = SqlStdOperatorTable.PATTERN_EXCLUDE.createCall(
                        Span.of(extra).end(this), extra);
                    e = SqlStdOperatorTable.PATTERN_CONCAT.createCall(
                        Span.of(e).end(this), e, extra);
                    return e;
                }
            )
        //----- FLINK MODIFICATION BEGIN -----
        // Extend negative event semantics with the operator "[^ ]"
        |
            <LBRACKET> <CARET> extra = RichPatternExpression() <RBRACKET> {
                    extra = SqlOperatorPattern.PATTERN_NEGATIVE.createCall(
                        Span.of(extra).end(this), extra);
                    e = SqlStdOperatorTable.PATTERN_CONCAT.createCall(
                        Span.of(e).end(this), e, extra);
                    return e;
                }
            // ----- FLINK MODIFICATION END -----
        )
        [
            <HOOK>
            {
                if (startNum.intValue(true) != endNum.intValue(true)) {
                    reluctant = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
                }
            }
        ]
    ]
    {
        if (startNum == null) {
            return e;
        } else {
            return SqlStdOperatorTable.PATTERN_QUANTIFIER.createCall(
                span().end(e), e, startNum, endNum, reluctant);
        }
    }
}

SqlNode RichPatternPrimary() :
{
    final Span s;
    SqlNode e;
    List<SqlNode> eList;
}
{
    (
        e = SimpleIdentifier()
    |
        <LPAREN> e = RichPatternExpression() <RPAREN>
    |
        <LBRACE> { s = span(); }
        <MINUS> e = RichPatternExpression()
        <MINUS> <RBRACE> {
            e = SqlStdOperatorTable.PATTERN_EXCLUDE.createCall(s.end(this), e);
        }
    //----- FLINK MODIFICATION BEGIN -----
    // Extend negative event semantics with the operator "[^ ]"
    |
        <LBRACKET> { s = span(); }
        <CARET> e = RichPatternExpression()
        <RBRACKET> {
            e = SqlOperatorPattern.PATTERN_NEGATIVE.createCall(s.end(this), e);
        }
    // ----- FLINK MODIFICATION END -----
    |
        (
            <PERMUTE> { s = span(); }
            <LPAREN>
            e = RichPatternExpression() {
                eList = new ArrayList<SqlNode>();
                eList.add(e);
            }
            (
                <COMMA>
                e = RichPatternExpression()
                {
                    eList.add(e);
                }
            )*
            <RPAREN> {
                e = SqlStdOperatorTable.PATTERN_PERMUTE.createCall(
                    s.end(this), eList);
            }
        )
    )
    {
        return e;
    }
}
