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

void TableColumnLine(TableTempWrapper wrapper) :
{
    SqlNode item;
}
{
    TableColumn(wrapper.columnList)
    |
    ComputedColumn(wrapper)
    |
    wrapper.watermark = Watermark()
    |
    wrapper.primaryKeyList = PrimaryKey()
    |
    UniqueKey(wrapper.uniqueKeysList)
    |
    IndexKey(wrapper.indexKeysList)
    |
    DimDetect()
}

void ComputedColumn(TableTempWrapper wrapper) :
{
    SqlNode var;
    SqlNode e;
    boolean hidden = false;
    SqlParserPos pos;
}
{
    var = SimpleIdentifier() {pos = getPos();}
    <AS>
    e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
        e = SqlStdOperatorTable.AS.createCall(Span.of(var, e).pos(), e, var);
        wrapper.columnList.add(e);
    }
}

String DimDetect() :
{
}
{
    <PERIOD> <FOR> <SYSTEM_TIME>
    {
        return "DIM";
    }
}

void TableColumn(List<SqlNode> list) :
{
    SqlParserPos pos;
    SqlIdentifier name;
    SqlDataTypeSpec type;
    SqlCharStringLiteral comment = null;
    boolean isHeader = false;
}
{
    { pos = getPos(); }

    name = SimpleIdentifier()
    type = DataType()
    [ <NOT> <NULL> { type = type.withNullable(false); } ]
    [ <HEADER> { isHeader = true; } ]
    [ <COMMENT> <QUOTED_STRING> {
        String p = SqlParserUtil.parseString(token.image);
        comment = SqlLiteral.createCharString(p, getPos());
    }]
    {
        SqlTableColumn tableColumn = new SqlTableColumn(name, type, comment, pos.plus(getPos()));
        tableColumn.setHeader(isHeader);
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
        columnName = SimpleIdentifier() {pkList.add(columnName);}
        (<COMMA>columnName = SimpleIdentifier() {pkList.add(columnName);})*
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
        columnName = SimpleIdentifier() {ukList.add(columnName);}
        (<COMMA>columnName = SimpleIdentifier() {ukList.add(columnName);})*
    <RPAREN>
    {
        SqlNodeList uk = new SqlNodeList(ukList, pos.plus(getPos()));
        list.add(uk);
    }
}

void IndexKey(List<IndexWrapper> list) :
{
    List<SqlNode> indexList = new ArrayList<SqlNode>();
    boolean unique = false;
    SqlParserPos pos;
    SqlIdentifier columnName;
}
{
    ( <UNIQUE> <INDEX> {unique = true;} | <INDEX> ) {pos = getPos();} <LPAREN>
        columnName = SimpleIdentifier() {indexList.add(columnName);}
        (<COMMA>columnName = SimpleIdentifier() {indexList.add(columnName);})*
    <RPAREN>
    {
        SqlNodeList indexKeys = new SqlNodeList(indexList, pos.plus(getPos()));
        IndexWrapper index = new IndexWrapper(unique, indexKeys);
        list.add(index);
    }
}

SqlWatermark Watermark() :
{
    SqlIdentifier watermarkName = null;
    SqlIdentifier columnName;
    SqlNode namedFunctionCall;

    SqlParserPos pos;
}
{
    <WATERMARK> {pos = getPos();} [watermarkName = SimpleIdentifier()]
    <FOR> columnName = SimpleIdentifier()
    <AS>
    namedFunctionCall = NamedFunctionCall()
    {
        return new SqlWatermark(watermarkName, columnName, namedFunctionCall, pos.plus(getPos()));
    }
}

/** Parses an EXTENDED or FORMATTED DESCRIBE statement. */
SqlNode SqlRichDescribe() :
{
    final Span s = Span.of();
    SqlIdentifier table;
    SqlIdentifier column = null;
    boolean isExtended = false;
    boolean isFormatted = false;
}
{
    <DESCRIBE>

     (<EXTENDED>  {isExtended = true; } | <FORMATTED> {isFormatted = true; })
     [<TABLE>] table = CompoundIdentifier() [ column = SimpleIdentifier()]
     {
         return new SqlRichDescribeTable(s.add(table).addIf(column).pos(), table, column,
           isExtended, isFormatted);
      }
}

SqlNode SqlAnalyzeTable():
{
   final SqlParserPos startPos;
   SqlIdentifier tableName;
   SqlNodeList columnList = SqlNodeList.EMPTY;
   SqlParserPos pos;
   boolean withColumns = false;
}
{
   <ANALYZE> <TABLE>
   { startPos = getPos(); }
    tableName = CompoundIdentifier()

    <COMPUTE> <STATISTICS> [ <FOR> <COLUMNS>
        {
        pos = getPos();
        withColumns = true;
        }
        [
        {
        List<SqlNode> columns = new ArrayList<SqlNode>();
        }
        SimpleIdentifierCommaList(columns)
        {
            for(SqlNode node : columns)
            {
                if (((SqlIdentifier)node).isStar())
                    throw new ParseException(String.format("Analyze Table's field list has a '*', which is invalid."));
            }
            columnList = new SqlNodeList(columns, pos.plus(getPos()));
        }
        ]
    ]
    {
       return new SqlAnalyzeTable(startPos.plus(getPos()), tableName, columnList, withColumns);
    }
}

SqlNode SqlCreateTable() :
{
    final SqlParserPos startPos;
    SqlIdentifier tableName;
    String tableType = null;
    SqlNodeList primaryKeyList = null;
    List<SqlNodeList> uniqueKeysList = null;
    List<IndexWrapper> indexesList = null;
    SqlNodeList columnList = SqlNodeList.EMPTY;
    SqlHiddenColumn procTime = null;
    SqlWatermark watermark = null;

    SqlNodeList propertyList = null;

    SqlParserPos pos;
}
{
    <CREATE> { startPos = getPos(); }

    <TABLE>

    tableName = CompoundIdentifier()
    [
        <LPAREN> { pos = getPos(); TableTempWrapper wrapper = new TableTempWrapper();}
        TableColumnLine(wrapper)
        (
            <COMMA> TableColumnLine(wrapper)
        )*
        {
            pos = pos.plus(getPos());
            columnList = new SqlNodeList(wrapper.columnList, pos);
            primaryKeyList = wrapper.primaryKeyList;
            uniqueKeysList = wrapper.uniqueKeysList;
            indexesList = wrapper.indexKeysList;
            watermark = wrapper.watermark;
            tableType = wrapper.tableType;
        }
        <RPAREN>
    ]
    [
        <WITH>
            {
                SqlNode property;
                List<SqlNode> proList = new ArrayList<SqlNode>();
                pos = getPos();
            }
            <LPAREN>
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
        {  propertyList = new SqlNodeList(proList, pos.plus(getPos())); }
    ]

    {
        return new SqlCreateTable(startPos.plus(getPos()), tableType, tableName, columnList, primaryKeyList, uniqueKeysList, indexesList, watermark, propertyList);
    }
}

SqlNode SqlCreateFunction() :
{
    SqlNode functionName = null;
    String className = null;
    SqlParserPos pos;

    SqlNode sample = null;
}
{
    <CREATE> { pos = getPos(); }

    <FUNCTION>

    functionName = CompoundIdentifier()

    <AS> sample = StringLiteral()

    {
        className = ((NlsString) SqlLiteral.value(sample)).getValue();
        return new SqlCreateFunction(pos, functionName, className);
    }
}



/** Parses an optional field list and makes sure no field is a "*". */
SqlNodeList ParseOptionalFieldList(String relType) :
{
    SqlNodeList fieldList;
}
{
    fieldList = ParseRequiredFieldList(relType)
    {
        return fieldList;
    }
    |
    {
        return SqlNodeList.EMPTY;
    }
}

/** Parses a required field list and makes sure no field is a "*". */
SqlNodeList ParseRequiredFieldList(String relType) :
{
    List<SqlNode> fieldList = new ArrayList<SqlNode>();
}
{
    <LPAREN>
        SimpleIdentifierCommaList(fieldList)
    <RPAREN>
    {
        for(SqlNode node : fieldList)
        {
            if (((SqlIdentifier)node).isStar())
                throw new ParseException(String.format("%s's field list has a '*', which is invalid.", relType));
        }
        return new SqlNodeList(fieldList, getPos());
    }
}

/**
 * Parses a create view or replace existing view statement.
 *   CREATE [OR REPLACE] VIEW view_name [ (field1, field2 ...) ] AS select_statement
 */
SqlNode SqlCreateOrReplaceView() :
{
    SqlParserPos pos;
    boolean replaceView = false;
    SqlIdentifier viewName;
    SqlNode query;
    SqlNodeList fieldList;
}
{
    <CREATE> { pos = getPos(); }
    [ <OR> <REPLACE> { replaceView = true; } ]
    <VIEW>
    viewName = CompoundIdentifier()
    fieldList = ParseOptionalFieldList("View")
    <AS>
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateView(pos, viewName, fieldList, query, replaceView);
    }
}
