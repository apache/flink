#### 编译

mvn clean install -DskipTests -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true


##本地 dubug
本地debug基本方法如下：

1、在jvm启动参数中添加远程调试参数
（1）如果是调试Client，可以将下述参数加到bin/flink脚本的最后一行中，形如：
JVM_REMOTE_DEBUG_ARGS='-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005'
exec $JAVA_RUN $JVM_ARGS $JVM_REMOTE_DEBUG_ARGS "${log_setting[@]}" -classpath "`manglePathList "$CC_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" org.apache.flink.client.CliFrontend "$@"
（2）如果是调试JobManager或TaskManager，可以在conf/flink-conf.yaml中添加：
env.java.opts: -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5006
2、启动flink client或jobmanager或taskmanager，此时程序会suspend等待debuger连接（通过suspend=y来配置）。
3、配置IDEA中的remote：host配置为localhost，配置port（参考1中的配置的address端口）。
4、在Flink源码中设置断点，连接远程host，然后就可以开始debug跟踪了。

