package org.apache.flink.runtime.rpc.akka;

/**
 * Created by Shimin Yang on 2018/9/21.
 */
public enum AkkaExecutorMode {
	/** Used by default, use fork-join-executor dispatcher **/
	FORK_JOIN_EXECUTOR,
	/** Use single thread (Pinned) dispatcher **/
	SINGLE_THREAD_EXECUTOR
}
