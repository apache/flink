package org.apache.flink.state.api.scala

import org.apache.flink.state.api.{WritableSavepoint, NewSavepoint => JNewSavepoint}

class NewSavepoint(savepoint: JNewSavepoint) extends WritableSavepoint[NewSavepoint](savepoint.metadata, savepoint.stateBackend)  {
