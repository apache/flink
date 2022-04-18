################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import json
import logging
import sys

from pyflink.common import Row
from pyflink.table import (DataTypes, TableEnvironment, EnvironmentSettings)
from pyflink.table.expressions import *
from pyflink.table.udf import udtf, udf, udaf, AggregateFunction, TableAggregateFunction, udtaf


def basic_operations():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    # define the source
    table = t_env.from_elements(
        elements=[
            (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}')
        ],
        schema=['id', 'data'])

    right_table = t_env.from_elements(elements=[(1, 18), (2, 30), (3, 25), (4, 10)],
                                      schema=['id', 'age'])

    table = table.add_columns(
                    col('data').json_value('$.name', DataTypes.STRING()).alias('name'),
                    col('data').json_value('$.tel', DataTypes.STRING()).alias('tel'),
                    col('data').json_value('$.addr.country', DataTypes.STRING()).alias('country')) \
                 .drop_columns(col('data'))
    table.execute().print()
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
    # | op |                   id |                           name |                            tel |                        country |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
    # | +I |                    1 |                          Flink |                            123 |                        Germany |
    # | +I |                    2 |                          hello |                            135 |                          China |
    # | +I |                    3 |                          world |                            124 |                            USA |
    # | +I |                    4 |                        PyFlink |                             32 |                          China |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+

    # limit the number of outputs
    table.limit(3).execute().print()
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
    # | op |                   id |                           name |                            tel |                        country |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
    # | +I |                    1 |                          Flink |                            123 |                        Germany |
    # | +I |                    2 |                          hello |                            135 |                          China |
    # | +I |                    3 |                          world |                            124 |                            USA |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+

    # filter
    table.filter(col('id') != 3).execute().print()
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
    # | op |                   id |                           name |                            tel |                        country |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
    # | +I |                    1 |                          Flink |                            123 |                        Germany |
    # | +I |                    2 |                          hello |                            135 |                          China |
    # | +I |                    4 |                        PyFlink |                             32 |                          China |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+

    # aggregation
    table.group_by(col('country')) \
         .select(col('country'), col('id').count, col('tel').cast(DataTypes.BIGINT()).max) \
         .execute().print()
    # +----+--------------------------------+----------------------+----------------------+
    # | op |                        country |               EXPR$0 |               EXPR$1 |
    # +----+--------------------------------+----------------------+----------------------+
    # | +I |                        Germany |                    1 |                  123 |
    # | +I |                            USA |                    1 |                  124 |
    # | +I |                          China |                    1 |                  135 |
    # | -U |                          China |                    1 |                  135 |
    # | +U |                          China |                    2 |                  135 |
    # +----+--------------------------------+----------------------+----------------------+

    # distinct
    table.select(col('country')).distinct() \
         .execute().print()
    # +----+--------------------------------+
    # | op |                        country |
    # +----+--------------------------------+
    # | +I |                        Germany |
    # | +I |                          China |
    # | +I |                            USA |
    # +----+--------------------------------+

    # join
    # Note that it still doesn't support duplicate column names between the joined tables
    table.join(right_table.rename_columns(col('id').alias('r_id')), col('id') == col('r_id')) \
         .execute().print()
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+----------------------+
    # | op |                   id |                           name |                            tel |                        country |                 r_id |                  age |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+----------------------+
    # | +I |                    4 |                        PyFlink |                             32 |                          China |                    4 |                   10 |
    # | +I |                    1 |                          Flink |                            123 |                        Germany |                    1 |                   18 |
    # | +I |                    2 |                          hello |                            135 |                          China |                    2 |                   30 |
    # | +I |                    3 |                          world |                            124 |                            USA |                    3 |                   25 |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+----------------------+

    # join lateral
    @udtf(result_types=[DataTypes.STRING()])
    def split(r: Row):
        for s in r.name.split("i"):
            yield s

    table.join_lateral(split.alias('a')) \
         .execute().print()
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
    # | op |                   id |                           name |                            tel |                        country |                              a |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
    # | +I |                    1 |                          Flink |                            123 |                        Germany |                             Fl |
    # | +I |                    1 |                          Flink |                            123 |                        Germany |                             nk |
    # | +I |                    2 |                          hello |                            135 |                          China |                          hello |
    # | +I |                    3 |                          world |                            124 |                            USA |                          world |
    # | +I |                    4 |                        PyFlink |                             32 |                          China |                           PyFl |
    # | +I |                    4 |                        PyFlink |                             32 |                          China |                             nk |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+

    # show schema
    table.print_schema()
    # (
    #   `id` BIGINT,
    #   `name` STRING,
    #   `tel` STRING,
    #   `country` STRING
    # )

    # show execute plan
    print(table.join_lateral(split.alias('a')).explain())
    # == Abstract Syntax Tree ==
    # LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{}])
    # :- LogicalProject(id=[$0], name=[JSON_VALUE($1, _UTF-16LE'$.name', FLAG(NULL), FLAG(ON EMPTY), FLAG(NULL), FLAG(ON ERROR))], tel=[JSON_VALUE($1, _UTF-16LE'$.tel', FLAG(NULL), FLAG(ON EMPTY), FLAG(NULL), FLAG(ON ERROR))], country=[JSON_VALUE($1, _UTF-16LE'$.addr.country', FLAG(NULL), FLAG(ON EMPTY), FLAG(NULL), FLAG(ON ERROR))])
    # :  +- LogicalTableScan(table=[[default_catalog, default_database, Unregistered_TableSource_249535355, source: [PythonInputFormatTableSource(id, data)]]])
    # +- LogicalTableFunctionScan(invocation=[*org.apache.flink.table.functions.python.PythonTableFunction$1f0568d1f39bef59b4c969a5d620ba46*($0, $1, $2, $3)], rowType=[RecordType(VARCHAR(2147483647) a)], elementType=[class [Ljava.lang.Object;])
    #
    # == Optimized Physical Plan ==
    # PythonCorrelate(invocation=[*org.apache.flink.table.functions.python.PythonTableFunction$1f0568d1f39bef59b4c969a5d620ba46*($0, $1, $2, $3)], correlate=[table(split(id,name,tel,country))], select=[id,name,tel,country,a], rowType=[RecordType(BIGINT id, VARCHAR(2147483647) name, VARCHAR(2147483647) tel, VARCHAR(2147483647) country, VARCHAR(2147483647) a)], joinType=[INNER])
    # +- Calc(select=[id, JSON_VALUE(data, _UTF-16LE'$.name', FLAG(NULL), FLAG(ON EMPTY), FLAG(NULL), FLAG(ON ERROR)) AS name, JSON_VALUE(data, _UTF-16LE'$.tel', FLAG(NULL), FLAG(ON EMPTY), FLAG(NULL), FLAG(ON ERROR)) AS tel, JSON_VALUE(data, _UTF-16LE'$.addr.country', FLAG(NULL), FLAG(ON EMPTY), FLAG(NULL), FLAG(ON ERROR)) AS country])
    #    +- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_249535355, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])
    #
    # == Optimized Execution Plan ==
    # PythonCorrelate(invocation=[*org.apache.flink.table.functions.python.PythonTableFunction$1f0568d1f39bef59b4c969a5d620ba46*($0, $1, $2, $3)], correlate=[table(split(id,name,tel,country))], select=[id,name,tel,country,a], rowType=[RecordType(BIGINT id, VARCHAR(2147483647) name, VARCHAR(2147483647) tel, VARCHAR(2147483647) country, VARCHAR(2147483647) a)], joinType=[INNER])
    # +- Calc(select=[id, JSON_VALUE(data, '$.name', NULL, ON EMPTY, NULL, ON ERROR) AS name, JSON_VALUE(data, '$.tel', NULL, ON EMPTY, NULL, ON ERROR) AS tel, JSON_VALUE(data, '$.addr.country', NULL, ON EMPTY, NULL, ON ERROR) AS country])
    #    +- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_249535355, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])


def sql_operations():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    # define the source
    table = t_env.from_elements(
        elements=[
            (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}')
        ],
        schema=['id', 'data'])

    t_env.sql_query("SELECT * FROM %s" % table) \
         .execute().print()
    # +----+----------------------+--------------------------------+
    # | op |                   id |                           data |
    # +----+----------------------+--------------------------------+
    # | +I |                    1 | {"name": "Flink", "tel": 12... |
    # | +I |                    2 | {"name": "hello", "tel": 13... |
    # | +I |                    3 | {"name": "world", "tel": 12... |
    # | +I |                    4 | {"name": "PyFlink", "tel": ... |
    # +----+----------------------+--------------------------------+

    # execute sql statement
    @udtf(result_types=[DataTypes.STRING(), DataTypes.INT(), DataTypes.STRING()])
    def parse_data(data: str):
        json_data = json.loads(data)
        yield json_data['name'], json_data['tel'], json_data['addr']['country']

    t_env.create_temporary_function('parse_data', parse_data)
    t_env.execute_sql(
        """
        SELECT *
        FROM %s, LATERAL TABLE(parse_data(`data`)) t(name, tel, country)
        """ % table
    ).print()
    # +----+----------------------+--------------------------------+--------------------------------+-------------+--------------------------------+
    # | op |                   id |                           data |                           name |         tel |                        country |
    # +----+----------------------+--------------------------------+--------------------------------+-------------+--------------------------------+
    # | +I |                    1 | {"name": "Flink", "tel": 12... |                          Flink |         123 |                        Germany |
    # | +I |                    2 | {"name": "hello", "tel": 13... |                          hello |         135 |                          China |
    # | +I |                    3 | {"name": "world", "tel": 12... |                          world |         124 |                            USA |
    # | +I |                    4 | {"name": "PyFlink", "tel": ... |                        PyFlink |          32 |                          China |
    # +----+----------------------+--------------------------------+--------------------------------+-------------+--------------------------------+

    # explain sql plan
    print(t_env.explain_sql(
        """
        SELECT *
        FROM %s, LATERAL TABLE(parse_data(`data`)) t(name, tel, country)
        """ % table
    ))
    # == Abstract Syntax Tree ==
    # LogicalProject(id=[$0], data=[$1], name=[$2], tel=[$3], country=[$4])
    # +- LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{1}])
    #    :- LogicalTableScan(table=[[default_catalog, default_database, Unregistered_TableSource_734856049, source: [PythonInputFormatTableSource(id, data)]]])
    #    +- LogicalTableFunctionScan(invocation=[parse_data($cor1.data)], rowType=[RecordType:peek_no_expand(VARCHAR(2147483647) f0, INTEGER f1, VARCHAR(2147483647) f2)])
    #
    # == Optimized Physical Plan ==
    # PythonCorrelate(invocation=[parse_data($1)], correlate=[table(parse_data(data))], select=[id,data,f0,f1,f2], rowType=[RecordType(BIGINT id, VARCHAR(2147483647) data, VARCHAR(2147483647) f0, INTEGER f1, VARCHAR(2147483647) f2)], joinType=[INNER])
    # +- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_734856049, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])
    #
    # == Optimized Execution Plan ==
    # PythonCorrelate(invocation=[parse_data($1)], correlate=[table(parse_data(data))], select=[id,data,f0,f1,f2], rowType=[RecordType(BIGINT id, VARCHAR(2147483647) data, VARCHAR(2147483647) f0, INTEGER f1, VARCHAR(2147483647) f2)], joinType=[INNER])
    # +- LegacyTableSourceScan(table=[[default_catalog, default_database, Unregistered_TableSource_734856049, source: [PythonInputFormatTableSource(id, data)]]], fields=[id, data])


def column_operations():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    # define the source
    table = t_env.from_elements(
        elements=[
            (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "tel": 124, "addr": {"country": "USA", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}')
        ],
        schema=['id', 'data'])

    # add columns
    table = table.add_columns(
        col('data').json_value('$.name', DataTypes.STRING()).alias('name'),
        col('data').json_value('$.tel', DataTypes.STRING()).alias('tel'),
        col('data').json_value('$.addr.country', DataTypes.STRING()).alias('country'))

    table.execute().print()
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
    # | op |                   id |                           data |                           name |                            tel |                        country |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
    # | +I |                    1 | {"name": "Flink", "tel": 12... |                          Flink |                            123 |                        Germany |
    # | +I |                    2 | {"name": "hello", "tel": 13... |                          hello |                            135 |                          China |
    # | +I |                    3 | {"name": "world", "tel": 12... |                          world |                            124 |                            USA |
    # | +I |                    4 | {"name": "PyFlink", "tel": ... |                        PyFlink |                             32 |                          China |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+

    # drop columns
    table = table.drop_columns(col('data'))
    table.execute().print()
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
    # | op |                   id |                           name |                            tel |                        country |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
    # | +I |                    1 |                          Flink |                            123 |                        Germany |
    # | +I |                    2 |                          hello |                            135 |                          China |
    # | +I |                    3 |                          world |                            124 |                            USA |
    # | +I |                    4 |                        PyFlink |                             32 |                          China |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+

    # rename columns
    table = table.rename_columns(col('tel').alias('telephone'))
    table.execute().print()
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
    # | op |                   id |                           name |                      telephone |                        country |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+
    # | +I |                    1 |                          Flink |                            123 |                        Germany |
    # | +I |                    2 |                          hello |                            135 |                          China |
    # | +I |                    3 |                          world |                            124 |                            USA |
    # | +I |                    4 |                        PyFlink |                             32 |                          China |
    # +----+----------------------+--------------------------------+--------------------------------+--------------------------------+

    # replace columns
    table = table.add_or_replace_columns(
        concat(col('id').cast(DataTypes.STRING()), '_', col('name')).alias('id'))
    table.execute().print()
    # +----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
    # | op |                             id |                           name |                      telephone |                        country |
    # +----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
    # | +I |                        1_Flink |                          Flink |                            123 |                        Germany |
    # | +I |                        2_hello |                          hello |                            135 |                          China |
    # | +I |                        3_world |                          world |                            124 |                            USA |
    # | +I |                      4_PyFlink |                        PyFlink |                             32 |                          China |
    # +----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+


def row_operations():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    # define the source
    table = t_env.from_elements(
        elements=[
            (1, '{"name": "Flink", "tel": 123, "addr": {"country": "Germany", "city": "Berlin"}}'),
            (2, '{"name": "hello", "tel": 135, "addr": {"country": "China", "city": "Shanghai"}}'),
            (3, '{"name": "world", "tel": 124, "addr": {"country": "China", "city": "NewYork"}}'),
            (4, '{"name": "PyFlink", "tel": 32, "addr": {"country": "China", "city": "Hangzhou"}}')
        ],
        schema=['id', 'data'])

    # map operation
    @udf(result_type=DataTypes.ROW([DataTypes.FIELD("id", DataTypes.BIGINT()),
                                    DataTypes.FIELD("country", DataTypes.STRING())]))
    def extract_country(input_row: Row):
        data = json.loads(input_row.data)
        return Row(input_row.id, data['addr']['country'])

    table.map(extract_country) \
         .execute().print()
    # +----+----------------------+--------------------------------+
    # | op |                   id |                        country |
    # +----+----------------------+--------------------------------+
    # | +I |                    1 |                        Germany |
    # | +I |                    2 |                          China |
    # | +I |                    3 |                          China |
    # | +I |                    4 |                          China |
    # +----+----------------------+--------------------------------+

    # flat_map operation
    @udtf(result_types=[DataTypes.BIGINT(), DataTypes.STRING()])
    def extract_city(input_row: Row):
        data = json.loads(input_row.data)
        yield input_row.id, data['addr']['city']

    table.flat_map(extract_city) \
         .execute().print()
    # +----+----------------------+--------------------------------+
    # | op |                   f0 |                             f1 |
    # +----+----------------------+--------------------------------+
    # | +I |                    1 |                         Berlin |
    # | +I |                    2 |                       Shanghai |
    # | +I |                    3 |                        NewYork |
    # | +I |                    4 |                       Hangzhou |
    # +----+----------------------+--------------------------------+

    # aggregate operation
    class CountAndSumAggregateFunction(AggregateFunction):

        def get_value(self, accumulator):
            return Row(accumulator[0], accumulator[1])

        def create_accumulator(self):
            return Row(0, 0)

        def accumulate(self, accumulator, input_row):
            accumulator[0] += 1
            accumulator[1] += int(input_row.tel)

        def retract(self, accumulator, input_row):
            accumulator[0] -= 1
            accumulator[1] -= int(input_row.tel)

        def merge(self, accumulator, accumulators):
            for other_acc in accumulators:
                accumulator[0] += other_acc[0]
                accumulator[1] += other_acc[1]

        def get_accumulator_type(self):
            return DataTypes.ROW(
                [DataTypes.FIELD("cnt", DataTypes.BIGINT()),
                 DataTypes.FIELD("sum", DataTypes.BIGINT())])

        def get_result_type(self):
            return DataTypes.ROW(
                [DataTypes.FIELD("cnt", DataTypes.BIGINT()),
                 DataTypes.FIELD("sum", DataTypes.BIGINT())])

    count_sum = udaf(CountAndSumAggregateFunction())
    table.add_columns(
            col('data').json_value('$.name', DataTypes.STRING()).alias('name'),
            col('data').json_value('$.tel', DataTypes.STRING()).alias('tel'),
            col('data').json_value('$.addr.country', DataTypes.STRING()).alias('country')) \
         .group_by(col('country')) \
         .aggregate(count_sum.alias("cnt", "sum")) \
         .select(col('country'), col('cnt'), col('sum')) \
         .execute().print()
    # +----+--------------------------------+----------------------+----------------------+
    # | op |                        country |                  cnt |                  sum |
    # +----+--------------------------------+----------------------+----------------------+
    # | +I |                          China |                    3 |                  291 |
    # | +I |                        Germany |                    1 |                  123 |
    # +----+--------------------------------+----------------------+----------------------+

    # flat_aggregate operation
    class Top2(TableAggregateFunction):

        def emit_value(self, accumulator):
            for v in accumulator:
                if v:
                    yield Row(v)

        def create_accumulator(self):
            return [None, None]

        def accumulate(self, accumulator, input_row):
            tel = int(input_row.tel)
            if accumulator[0] is None or tel > accumulator[0]:
                accumulator[1] = accumulator[0]
                accumulator[0] = tel
            elif accumulator[1] is None or tel > accumulator[1]:
                accumulator[1] = tel

        def get_accumulator_type(self):
            return DataTypes.ARRAY(DataTypes.BIGINT())

        def get_result_type(self):
            return DataTypes.ROW(
                [DataTypes.FIELD("tel", DataTypes.BIGINT())])

    top2 = udtaf(Top2())
    table.add_columns(
            col('data').json_value('$.name', DataTypes.STRING()).alias('name'),
            col('data').json_value('$.tel', DataTypes.STRING()).alias('tel'),
            col('data').json_value('$.addr.country', DataTypes.STRING()).alias('country')) \
        .group_by(col('country')) \
        .flat_aggregate(top2) \
        .select(col('country'), col('tel')) \
        .execute().print()
    # +----+--------------------------------+----------------------+
    # | op |                        country |                  tel |
    # +----+--------------------------------+----------------------+
    # | +I |                          China |                  135 |
    # | +I |                          China |                  124 |
    # | +I |                        Germany |                  123 |
    # +----+--------------------------------+----------------------+


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    basic_operations()
    sql_operations()
    column_operations()
    row_operations()
