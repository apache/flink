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

void TableColumn(TableCreationContext context) :
{
}
{
    (
        TableColumn2(context.columnList)
    |
        context.primaryKeyList = PrimaryKey()
    |
        UniqueKey(context.uniqueKeysList)
    |
        ComputedColumn(context)
    )
}

void ComputedColumn(TableCreationContext context) :
{
    SqlNode identifier;
    SqlNode expr;
    boolean hidden = false;
    SqlParserPos pos;
}
{
    identifier = SimpleIdentifier() {pos = getPos();}
    <AS>
    expr = Expression(ExprContext.ACCEPT_SUB_QUERY) {
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
    type = DataType()
    (
        <NULL> { type = type.withNullable(true); }
    |
        <NOT> <NULL> { type = type.withNullable(false); }
    |
        { type = type.withNullable(true); }
    )
    [ <COMMENT> <QUOTED_STRING> {
        String p = SqlParserUtil.parseString(token.image);
        comment = SqlLiteral.createCharString(p, getPos());
    }]
    {
        SqlTableColumn tableColumn = new SqlTableColumn(name, type, comment, getPos());
        list.add(tableColumn);
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

SqlNode PropertyValue() :
{
    SqlIdentifier key;
    SqlNode value;
    SqlParserPos pos;
}
{
    key = CompoundIdentifier()
    { pos = getPos(); }
    <EQ> value = StringLiteral()
    {
        return new SqlProperty(key, value, getPos());
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
        property = PropertyValue()
        {
            proList.add(property);
        }
        (
            <COMMA> property = PropertyValue()
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
    SqlNodeList primaryKeyList = null;
    List<SqlNodeList> uniqueKeysList = null;
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
    {
        return new SqlCreateTable(startPos.plus(getPos()),
                tableName,
                columnList,
                primaryKeyList,
                uniqueKeysList,
                propertyList,
                partitionColumns,
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

    [<IF> <EXISTS> { ifExists = true; } ]

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
            if (!((FlinkSqlConformance) this.conformance).allowInsertOverwrite()) {
                throw new ParseException("OVERWRITE expression is only allowed for HIVE dialect");
            } else if (RichSqlInsert.isUpsert(keywords)) {
                throw new ParseException("OVERWRITE expression is only used with INSERT mode");
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
        <PARTITION> PartitionSpecCommaList(partitionList) {
            if (!((FlinkSqlConformance) this.conformance).allowInsertIntoPartition()) {
                throw new ParseException("PARTITION expression is only allowed for HIVE dialect");
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
    [<IF> <EXISTS> { ifExists = true; } ]
    viewName = CompoundIdentifier()
    {
        return new SqlDropView(s.pos(), viewName, ifExists);
    }
}

SqlIdentifier SqlArrayType() :
{
    SqlParserPos pos;
    SqlDataTypeSpec elementType;
}
{
    <ARRAY> { pos = getPos(); }
    <LT> elementType = DataType()
    <GT>
    {
        return new SqlArrayType(pos, elementType);
    }
}

SqlIdentifier SqlMapType() :
{
    SqlParserPos pos;
    SqlDataTypeSpec keyType;
    SqlDataTypeSpec valType;
}
{
    <MAP> { pos = getPos(); }
    <LT> keyType = DataType()
    <COMMA> valType = DataType()
    <GT>
    {
        return new SqlMapType(pos, keyType, valType);
    }
}

SqlIdentifier SqlRowType() :
{
    SqlParserPos pos;
    List<SqlIdentifier> fieldNames = new ArrayList<SqlIdentifier>();
    List<SqlDataTypeSpec> fieldTypes = new ArrayList<SqlDataTypeSpec>();
}
{
    <ROW> { pos = getPos(); SqlIdentifier fName; SqlDataTypeSpec fType;}
    <LT>
    fName = SimpleIdentifier() <COLON> fType = DataType()
    { fieldNames.add(fName); fieldTypes.add(fType); }
    (
        <COMMA>
        fName = SimpleIdentifier() <COLON> fType = DataType()
        { fieldNames.add(fName); fieldTypes.add(fType); }
    )*
    <GT>
    {
        return new SqlRowType(pos, fieldNames, fieldTypes);
    }
}
