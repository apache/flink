- You're a respected open source apache flink maintainer.

- Test your changes with one appropriate existing test using this format just to check if we have no compilation errors "cd /Users/gdemorais/qdev/flink2 && ./mvnw test -Dtest="MultiJoinTest#testTwoWayJoinWithUnion" -pl flink-table/flink-table-planner -q -Dcheckstyle.skip -Drat.skip -Dscalastyle.skip -Denforcer.skip=true -Pgenerate-config-docs -Dspotless.check.skip=true"
- If you cannot run your tests because they get stuck downloading dependencies. You can run "./mvnw clean install -T1C -DskipTests -Pfast -Dcheckstyle.skip -Drat.skip -Dscalastyle.skip -Denforcer.skip=true -Pgenerate-config-docs -Dspotless.check.skip=true -DskipITs=true -Dmaven.javadoc.skip=true -Djapicmp.skip=true -Pskip-webui-build -T4 -fn" to build first. Just do this once and if necessary because it takes over 5 minutes!
- If you have to install only one module, also use all the available flags to speed up the process. E.g. "./mvnw install -pl flink-table/flink-table-common -T1C -DskipTests -Pfast -Dcheckstyle.skip -Drat.skip -Dscalastyle.skip -Denforcer.skip=true -Pgenerate-config-docs -Dspotless.check.skip=true -DskipITs=true -Dmaven.javadoc.skip=true -Djapicmp.skip=true -Pskip-webui-build -T4"

- We always want to keep the things we do in "eval" for functions and in the processRecord methods in operators to a minimum since this is a hot path for flink which is executed millions of times. Always do what you can in the constructor, intialize things else where. 
    
