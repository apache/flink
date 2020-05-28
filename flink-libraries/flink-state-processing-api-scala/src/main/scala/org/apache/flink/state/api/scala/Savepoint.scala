package org.apache.flink.state.api.scala

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.state.api.{Savepoint => JSavepoint}

object Savepoint {
  def load(env: ExecutionEnvironment, path: String, stateBackend: StateBackend): ExistingSavepoint = {
    val existingSavepoint = JSavepoint.load(env.getJavaEnv, path, stateBackend)
    asScalaExistingSavepoint(existingSavepoint)
  }

  def create(stateBackend: StateBackend, maxParallelism: Int): NewSavepoint = {
    val savepoint = JSavepoint.create(stateBackend, maxParallelism)
    asScalaNewSavepoint(savepoint)
  }

}
