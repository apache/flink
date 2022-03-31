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
    [<LANGUAGE>
        (
            <JAVA>  { functionLanguage = "JAVA"; }
        |
            <SCALA> { functionLanguage = "SCALA"; }
        |
            <SQL>   { functionLanguage = "SQL"; }
        |
            <PYTHON>   { functionLanguage = "PYTHON"; }
        )
    ]
    {
        return new SqlCreateFunction(s.pos(), functionIdentifier, functionClassName, functionLanguage,
                ifNotExists, isTemporary, isSystemFunction);
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
    SqlTableConstraint constraint;
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
        <ADD> constraint = TableConstraint() {
            return new SqlAlterTableAddConstraint(
                        tableIdentifier,
                        constraint,
                        startPos.plus(getPos()));
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
void TypedColumn(TableCreationContext context) :
{
    SqlIdentifier name;
    SqlParserPos pos;
    SqlDataTypeSpec type;
}
{
    name = SimpleIdentifier() {pos = getPos();}
    type = ExtendedDataType()
    (
        MetadataColumn(context, name, type)
    |
        RegularColumn(context, name, type)
    )
}

/** Parses {@code column_name AS expr [COMMENT 'comment']}. */
void ComputedColumn(TableCreationContext context) :
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
    }
}

/** Parses {@code column_name column_data_type METADATA [FROM 'alias_name'] [VIRTUAL] [COMMENT 'comment']}. */
void MetadataColumn(TableCreationContext context, SqlIdentifier name, SqlDataTypeSpec type) :
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
    }
}

/** Parses {@code column_name column_data_type [constraint] [COMMENT 'comment']}. */
void RegularColumn(TableCreationContext context, SqlIdentifier name, SqlDataTypeSpec type) :
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
                tableLike,
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
    source = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
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
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
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
