package org.apache.flink.table.api;

public class Fetch {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tbEnv = TableEnvironment.create(settings);
        Table t1 = tbEnv.fromValues("1", "2", "3", "4", "5", "6", "7", "8", "9");
        Table t2 = t1.limit(5).limit(6);
        tbEnv.executeSql("create table print(c1 int) with ('connector' = 'print')");
        tbEnv.insertInto("print",t2);
        tbEnv.execute("");
    }
}
