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

SqlCharStringLiteral createStringLiteral(String s, SqlParserPos pos) :
{
}
{
    { return SqlLiteral.createCharString(SqlParserUtil.parseString(s), pos); }
}

/**
* Parse a "Show Databases" metadata query command.
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
*/
SqlCreate SqlCreateDatabase(Span s, boolean replace) :
{
    SqlParserPos startPos;
    SqlIdentifier databaseName;
    SqlCharStringLiteral comment = null;
    SqlCharStringLiteral location = null;
    SqlNodeList propertyList;
    boolean ifNotExists = false;
}
{
    ( <DATABASE> | <SCHEMA> )
      {
        startPos = getPos();
        propertyList = new SqlNodeList(startPos);
      }
    [ LOOKAHEAD(3)
      <IF> <NOT> <EXISTS> { ifNotExists = true; }
    ]
    databaseName = CompoundIdentifier()
    [ <COMMENT> <QUOTED_STRING>
        {
            comment = createStringLiteral(token.image, getPos());
        }
    ]
    [
      <LOCATION> <QUOTED_STRING>
        {
            location = createStringLiteral(token.image, getPos());
        }
    ]
    [
        <WITH> <DBPROPERTIES>
        propertyList = TableProperties()
    ]

    { return new SqlCreateHiveDatabase(startPos.plus(getPos()),
                    databaseName,
                    propertyList,
                    comment,
                    location,
                    ifNotExists); }

}

SqlAlterDatabase SqlAlterDatabase() :
{
    SqlParserPos startPos;
    SqlIdentifier databaseName;
    SqlNodeList propertyList;
    SqlAlterDatabase alter;
    String ownerType;
    SqlIdentifier ownerName;
}
{
    <ALTER> ( <DATABASE> | <SCHEMA> ) { startPos = getPos(); }
    databaseName = CompoundIdentifier()
    <SET>
    (
        <DBPROPERTIES>
        propertyList = TableProperties()
            {
                alter = new SqlAlterHiveDatabaseProps(startPos.plus(getPos()), databaseName, propertyList);
            }
        |
        <LOCATION> <QUOTED_STRING>
            {
                SqlCharStringLiteral location = createStringLiteral(token.image, getPos());
                alter = new SqlAlterHiveDatabaseLocation(startPos.plus(getPos()), databaseName, location);
            }
        |
        <OWNER>
            (   <USER> { ownerType = SqlAlterHiveDatabaseOwner.USER_OWNER; }
                |
                <ROLE> { ownerType = SqlAlterHiveDatabaseOwner.ROLE_OWNER; }
            )
            ownerName = SimpleIdentifier()
            {
                alter = new SqlAlterHiveDatabaseOwner(startPos.plus(getPos()), databaseName, ownerType, ownerName);
            }
    )

    {
        return alter;
    }
}

SqlDrop SqlDropDatabase(Span s, boolean replace) :
{
    SqlIdentifier databaseName = null;
    boolean ifExists = false;
    boolean cascade = false;
}
{
    ( <DATABASE> | <SCHEMA> )

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
    <DESCRIBE> ( <DATABASE> | <SCHEMA> ) { pos = getPos();}
    [ <EXTENDED> { isExtended = true;} ]
    databaseName = CompoundIdentifier()
    {
        return new SqlDescribeDatabase(pos, databaseName, isExtended);
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