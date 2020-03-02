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

SqlDescribeCatalog SqlDescribeCatalog() :
{
    SqlIdentifier catalogName;
    SqlParserPos pos;
}
{
    <DESCRIBE> <CATALOG> { pos = getPos();}
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

/**
* Parse a "Show Catalogs" metadata query command.
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
* CREATE DATABASE database_name [COMMENT database_comment] [WITH (property_name=property_value, ...)];
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
    [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
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

    (
        <IF> <EXISTS> { ifExists = true; }
    |
        { ifExists = false; }
    )

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
    <DESCRIBE> <DATABASE> { pos = getPos();}
    [ <EXTENDED> { isExtended = true;} ]
    databaseName = CompoundIdentifier()
    {
        return new SqlDescribeDatabase(pos, databaseName, isExtended);
    }

}

SqlCreate SqlCreateFunction(Span s, boolean replace) :
{
    SqlIdentifier functionIdentifier = null;
    SqlCharStringLiteral functionClassName = null;
    String functionLanguage = null;
    boolean ifNotExists = false;
    boolean isTemporary = false;
    boolean isSystemFunction = false;
}
{
    [ <TEMPORARY>   {isTemporary = true;}
        [ <SYSTEM>   { isSystemFunction = true; } ]
    ]

    <FUNCTION>

    [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ]

    functionIdentifier = CompoundIdentifier()

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
        )
    ]
    {
        return new SqlCreateFunction(s.pos(), functionIdentifier, functionClassName, functionLanguage,
                ifNotExists, isTemporary, isSystemFunction);
    }
}

SqlDrop SqlDropFunction(Span s, boolean replace) :
{
    SqlIdentifier functionIdentifier = null;
    boolean ifExists = false;
    boolean isTemporary = false;
    boolean isSystemFunction = false;
}
{
    [ <TEMPORARY> {isTemporary = true;}
        [  <SYSTEM>   { isSystemFunction = true; }  ]
    ]
    <FUNCTION>

    [ <IF> <EXISTS> { ifExists = true; } ]

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

    [ <IF> <EXISTS> { ifExists = true; } ]

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
        )
    ]
    {
        return new SqlAlterFunction(startPos.plus(getPos()), functionIdentifier, functionClassName,
            functionLanguage, ifExists, isTemporary, isSystemFunction);
    }
}

SqlShowFunctions SqlShowFunctions() :
{
    SqlIdentifier database = null;
    SqlParserPos pos;
}
{
    <SHOW> <FUNCTIONS> { pos = getPos();}
    [database = CompoundIdentifier()]
    {
        return new SqlShowFunctions(pos, database);
    }
}

/**
* Parse a "Show Tables" metadata query command.
*/
SqlShowTables SqlShowTables() :
{
}
{
    <SHOW> <TABLES>
    {
        return new SqlShowTables(getPos());
    }
}

/**
 * DESCRIBE [ EXTENDED] [[catalogName.] dataBasesName].tableName sql call.
 * Here we add Rich in className to distinguish from calcite's original SqlDescribeTable.
 */
SqlRichDescribeTable SqlRichDescribeTable() :
{
    SqlIdentifier tableName;
    SqlParserPos pos;
    boolean isExtended = false;
}
{
    <DESCRIBE> { pos = getPos();}
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
        <SET>
        propertyList = TableProperties()
        {
            return new SqlAlterTableProperties(
                        startPos.plus(getPos()),
                        tableIdentifier,
                        propertyList);
        }
    )
}

void TableColumn(TableCreationContext context) :
{
}
{
    (LOOKAHEAD(2)
        TableColumn2(context.columnList)
    |
        context.primaryKeyList = PrimaryKey()
    |
        UniqueKey(context.uniqueKeysList)
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

void ComputedColumn(TableCreationContext context) :
{
    SqlNode identifier;
    SqlNode expr;
    SqlParserPos pos;
}
{
    identifier = SimpleIdentifier() {pos = getPos();}
    <AS>
    expr = Expression(ExprContext.ACCEPT_NON_QUERY) {
        expr = SqlStdOperatorTable.AS.createCall(Span.of(identifier, expr).pos(), expr, identifier);
        context.columnList.add(expr);
    }
}

void TableColumn2(List<SqlNode> list) :
{
    SqlParserPos pos;
    SqlIdentifier name;
    SqlDataTypeSpec type;
    SqlCharStringLiteral comment = null;
}
{
    name = SimpleIdentifier()
    type = ExtendedDataType()
    [ <COMMENT> <QUOTED_STRING> {
        String p = SqlParserUtil.parseString(token.image);
        comment = SqlLiteral.createCharString(p, getPos());
    }]
    {
        SqlTableColumn tableColumn = new SqlTableColumn(name, type, comment, getPos());
        list.add(tableColumn);
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

SqlNodeList PrimaryKey() :
{
    List<SqlNode> pkList = new ArrayList<SqlNode>();

    SqlParserPos pos;
    SqlIdentifier columnName;
}
{
    <PRIMARY> { pos = getPos(); } <KEY> <LPAREN>
        columnName = SimpleIdentifier() { pkList.add(columnName); }
        (<COMMA> columnName = SimpleIdentifier() { pkList.add(columnName); })*
    <RPAREN>
    {
        return new SqlNodeList(pkList, pos.plus(getPos()));
    }
}

void UniqueKey(List<SqlNodeList> list) :
{
    List<SqlNode> ukList = new ArrayList<SqlNode>();
    SqlParserPos pos;
    SqlIdentifier columnName;
}
{
    <UNIQUE> { pos = getPos(); } <LPAREN>
        columnName = SimpleIdentifier() { ukList.add(columnName); }
        (<COMMA> columnName = SimpleIdentifier() { ukList.add(columnName); })*
    <RPAREN>
    {
        SqlNodeList uk = new SqlNodeList(ukList, pos.plus(getPos()));
        list.add(uk);
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

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final SqlParserPos startPos = s.pos();
    SqlIdentifier tableName;
    SqlNodeList primaryKeyList = SqlNodeList.EMPTY;
    List<SqlNodeList> uniqueKeysList = new ArrayList<SqlNodeList>();
    SqlWatermark watermark = null;
    SqlNodeList columnList = SqlNodeList.EMPTY;
	SqlCharStringLiteral comment = null;

    SqlNodeList propertyList = SqlNodeList.EMPTY;
    SqlNodeList partitionColumns = SqlNodeList.EMPTY;
    SqlParserPos pos = startPos;
}
{
    <TABLE>

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
            primaryKeyList = ctx.primaryKeyList;
            uniqueKeysList = ctx.uniqueKeysList;
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
        partitionColumns = ParenthesizedSimpleIdentifierList() {
            if (!((FlinkSqlConformance) this.conformance).allowCreatePartitionedTable()) {
                throw SqlUtil.newContextException(getPos(),
                    ParserResource.RESOURCE.createPartitionedTableIsOnlyAllowedForHive());
            }
        }
    ]
    [
        <WITH>
        propertyList = TableProperties()
    ]
    {
        return new SqlCreateTable(startPos.plus(getPos()),
                tableName,
                columnList,
                primaryKeyList,
                uniqueKeysList,
                propertyList,
                partitionColumns,
                watermark,
                comment);
    }
}

SqlDrop SqlDropTable(Span s, boolean replace) :
{
    SqlIdentifier tableName = null;
    boolean ifExists = false;
}
{
    <TABLE>

    (
        <IF> <EXISTS> { ifExists = true; }
    |
        { ifExists = false; }
    )

    tableName = CompoundIdentifier()

    {
         return new SqlDropTable(s.pos(), tableName, ifExists);
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
    table = CompoundIdentifier()
    [
        LOOKAHEAD(5)
        [ <EXTEND> ]
        extendList = ExtendList() {
            table = extend(table, extendList);
        }
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
    [
        <PARTITION> PartitionSpecCommaList(partitionList)
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
* Parses a create view or replace existing view statement.
*   CREATE [OR REPLACE] VIEW view_name [ (field1, field2 ...) ] AS select_statement
*/
SqlCreate SqlCreateView(Span s, boolean replace) : {
    SqlIdentifier viewName;
    SqlCharStringLiteral comment = null;
    SqlNode query;
    SqlNodeList fieldList = SqlNodeList.EMPTY;
}
{
    <VIEW>
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
        return new SqlCreateView(s.pos(), viewName, fieldList, query, replace, comment);
    }
}

SqlDrop SqlDropView(Span s, boolean replace) :
{
    SqlIdentifier viewName = null;
    boolean ifExists = false;
}
{
    <VIEW>
    (
        <IF> <EXISTS> { ifExists = true; }
    |
        { ifExists = false; }
    )
    viewName = CompoundIdentifier()
    {
        return new SqlDropView(s.pos(), viewName, ifExists);
    }
}

/**
* A sql type name extended basic data type, it has a counterpart basic
* sql type name but always represents as a special alias compared with the standard name.
*
* <p>For example, STRING is synonym of VARCHAR(INT_MAX)
* and BYTES is synonym of VARBINARY(INT_MAX).
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
    )
    {
        return new ExtendedSqlBasicTypeNameSpec(typeAlias, typeName, precision, getPos());
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
    boolean nullable = true;
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
