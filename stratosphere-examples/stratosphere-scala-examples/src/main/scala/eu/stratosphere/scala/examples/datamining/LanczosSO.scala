package eu.stratosphere.scala.examples.datamining;
///**
// * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// * specific language governing permissions and limitations under the License.
// */
//
//package eu.stratosphere.pact4s.examples.datamining
//
//import scala.math._
//import eu.stratosphere.scala._
//import eu.stratosphere.scala.operators.DelimitedDataSinkFormat
//import eu.stratosphere.scala.operators.DelimitedDataSourceFormat
//import eu.stratosphere.api.plan.PlanAssemblerDescription
//import eu.stratosphere.client.LocalExecutor
//import eu.stratosphere.scala.analysis.GlobalSchemaPrinter
//
//
//object RunLanczos {
//  def main(args: Array[String]) {
//    val plan = KMeans.getPlan(20,
//      "file:///home/aljoscha/kmeans-points",
//      "file:///home/aljoscha/kmeans-clusters",
//      "file:///home/aljoscha/kmeans-output")
//
//    GlobalSchemaPrinter.printSchema(plan)
//    LocalExecutor.execute(plan)
//
//    System.exit(0)
//  }
//}
//
//class LanczosDescriptor extends ScalaPlanAssembler with PlanAssemblerDescription {
//  override def getDescription = "[-k <int:10>] [-m <int:10>] [-eps <double:0.05>] -A <file> -b <file> -lamba <file> -Y <file>"
//
//  override def getScalaPlan(args: Args) = LanczosSO.getPlan(args("k", "10").toInt, args("m", "10").toInt, args("eps", "0.05").toDouble, args("A"), args("b"), args("lambda"), args("Y"))
//}
//
//abstract sealed class Cell {
//  val row: Int; val col: Int; val value: Double
//  def isAlpha = false; def isBeta = false; def isQ = false; def isV = false; def isOther = false
//}
//case class AlphaCell(row: Int, col: Int, value: Double) extends Cell { override def isAlpha = true }
//case class BetaCell(row: Int, col: Int, value: Double) extends Cell { override def isBeta = true }
//case class QCell(row: Int, col: Int, value: Double) extends Cell { override def isQ = true }
//case class VCell(row: Int, col: Int, value: Double) extends Cell { override def isV = true }
//case class OtherCell(row: Int, col: Int, value: Double) extends Cell { override def isOther = true }
//
//  
//object LanczosSO {
//  
//
//  def getPlan(k: Int, m: Int, ε: Double, inputA: String, inputB: String, outputLambda: String, outputY: String) = {
//
//  val A = DataSource(inputA, DelimitedDataSourceFormat(parseCell))
//  val b = DataSource(inputB, DelimitedDataSourceFormat(parseCell))
//
//  val αʹβʹvʹ = mulVS(b, normV(b) map { 1 / _ }) flatMap { c =>
//    val v0: Cell = VCell(0, c.col, 0)
//    val v1: Cell = VCell(1, c.col, c.value)
//
//    if (c.col == 0)
//      Seq(BetaCell(0, 0, 0), v0, v1)
//    else
//      Seq(v0, v1)
//  }
//
//  val (α, β, v) = splitαβv(stepI repeat (n = m, s0 = αʹβʹvʹ))
//
//  val t = triDiag(α, β filter { _.col > 0 })
//  val (q, d) = decompose(t)
//
//  val diag = d filter { c => c.col == c.row }
//  val λ = diag map { (0, _) } groupBy { _._1 } reduce { eigenValues =>
//    val highToLow = eigenValues.map(_._2).toSeq.sortBy(c => abs(c.value))(Ordering[Double].reverse)
//    highToLow take (k) map { c => OtherCell(0, c.col, c.value): Cell }
//  } flatMap { c => c }
//
//  val y = mulMM(v, q join λ on { _.col } isEqualTo { _.col } map { (q, _) => q })
//
//  val λsink = λ.write(outputLambda, DelimitedDataSinkFormat(formatCell))
//  val ysink = y.write(outputY, DelimitedDataSinkFormat(formatCell))
//  
//  new ScalaPlan(Seq(λsink, ysink), "LanczosSO")
//  }
//
//  override def outputs = Seq(λsink <~ λ, ysink <~ y)
//
//  def stepI = (αʹβʹv: DataStream[Cell]) => {
//
//    val i = 1 // need current iteration!
//
//    val (αʹ, βʹ, v) = splitαβv(αʹβʹv)
//
//    val vᵢ = v filter { _.row == i }
//    val vᵢᐨ = v filter { _.row == i - 1 }
//    val βᵢᐨ = βʹ filter { c => c.col == i - 1 } map { _.value }
//
//    val vᵢᐩʹʹʹ = mulMV(A, vᵢ)
//    val αᵢ = dot(vᵢ, vᵢᐩʹʹʹ)
//    val vᵢᐩʹʹ = sub(vᵢᐩʹʹʹ, sub(mulVS(vᵢᐨ, βᵢᐨ), mulVS(vᵢ, αᵢ)))
//    val βᵢʹ = normV(vᵢᐩʹʹ)
//
//    val α = αʹ union (αᵢ map { AlphaCell(0, i, _): Cell })
//    val t = triDiag(α, βʹ union (βᵢʹ map { BetaCell(0, i, _): Cell }))
//    val (q, d) = decompose(t)
//
//    val βᵢʹvᵢᐩʹʹ = (βᵢʹ map { x => BetaCell(0, 0, x): Cell }) union vᵢᐩʹʹ
//
//    val βᵢvᵢᐩʹ = stepJ(i, v, q, normM(t)) repeat (n = i, s0 = βᵢʹvᵢᐩʹʹ)
//    val βᵢ = βᵢvᵢᐩʹ filter { _.isBeta } map { _.value }
//    val vᵢᐩʹ = βᵢvᵢᐩʹ filter { _.isV }
//
//    val β = βʹ union (βᵢ map { BetaCell(0, i, _): Cell })
//    val vᵢᐩ = mulVS(vᵢᐩʹ, βᵢ map { 1 / _ })
//    val vᐩ = v union vᵢᐩ
//
//    α union β union vᐩ
//  }
//
//  def stepJ = (i: Int, v: DataStream[Cell], q: DataStream[Cell], normT: DataStream[Double]) => {
//
//    val qvt = v union q union (normT map { t => OtherCell(0, 0, t): Cell })
//
//    (βᵢʹvᵢᐩʹʹ: DataStream[Cell]) => {
//
//      val j = 1 // need current iteration!
//
//      // Would it be better to union+reduce instead of cogroup here?
//      (qvt map { (0, _) }) cogroup (βᵢʹvᵢᐩʹʹ map { (0, _) }) on { _._1 } isEqualTo { _._1 } flatMap { (qvt, βᵢʹvᵢᐩʹʹ) =>
//
//        val (q, vt) = qvt.toSeq map { _._2 } partition { _.isQ }
//        val (v, t) = vt partition { _.isV }
//        val qij = q.find(c => c.row == i && c.col == j).get.value
//        val normT = t.head.value
//
//        val (βᵢʹs, vᵢᐩʹʹ) = βᵢʹvᵢᐩʹʹ.toSeq map { _._2 } partition { _.isBeta }
//        val βᵢʹ = βᵢʹs.head.value
//
//        if (βᵢʹ * abs(qij) <= sqrt(ε) * normT) {
//          val r = mulMV(v, q filter { _.col == j })
//
//          val vᵢᐩʹ = sub(vᵢᐩʹʹ, mulVS(r, dot(r, vᵢᐩʹʹ)))
//          val βᵢ = BetaCell(0, i, norm(vᵢᐩʹ)): Cell
//
//          βᵢ +: vᵢᐩʹ
//
//        } else {
//          βᵢʹs ++ vᵢᐩʹʹ
//        }
//      }
//    }
//  }
//
//  def splitαβv(αβv: DataStream[Cell]) = (αβv filter { _.isAlpha }, αβv filter { _.isBeta }, αβv filter { _.isV })
//
//  def triDiag(α: DataStream[Cell], β: DataStream[Cell]) = {
//    val diag = α map { c => OtherCell(c.col, c.col, c.value): Cell }
//    val lower = β map { c => OtherCell(c.col, c.col - 1, c.value): Cell }
//    val upper = β map { c => OtherCell(c.col - 1, c.col, c.value): Cell }
//    diag union lower union upper
//  }
//
//  def normV(x: DataStream[Cell]): DataStream[Double] = x map { _.value} //throw new RuntimeException("Not implemented")
//  def normM(x: DataStream[Cell]): DataStream[Double] = x map { _.value} //throw new RuntimeException("Not implemented")
//  def dot(x: DataStream[Cell], y: DataStream[Cell]): DataStream[Double] = x map { _.value} //throw new RuntimeException("Not implemented")
//  def sub(x: DataStream[Cell], y: DataStream[Cell]): DataStream[Cell] = x //throw new RuntimeException("Not implemented")
//  def mulSS(x: DataStream[Double], y: DataStream[Double]): DataStream[Double] = x //throw new RuntimeException("Not implemented")
//  def mulVS(x: DataStream[Cell], y: DataStream[Double]): DataStream[Cell] = x //throw new RuntimeException("Not implemented")
//  def mulMM(x: DataStream[Cell], y: DataStream[Cell]): DataStream[Cell] = x //throw new RuntimeException("Not implemented")
//  def mulMV(x: DataStream[Cell], y: DataStream[Cell]): DataStream[Cell] = x //throw new RuntimeException("Not implemented")
//  def decompose(t: DataStream[Cell]): (DataStream[Cell], DataStream[Cell]) = (t, t) // throw new RuntimeException("Not implemented")
//
//  def norm(x: Seq[Cell]): Double = 0 //throw new RuntimeException("Not implemented")
//  def dot(x: Seq[Cell], y: Seq[Cell]): Double = 0 //throw new RuntimeException("Not implemented")
//  def sub(x: Seq[Cell], y: Seq[Cell]): Seq[Cell] = Seq() //throw new RuntimeException("Not implemented")
//  def mulVS(x: Seq[Cell], y: Double): Seq[Cell] = Seq() //throw new RuntimeException("Not implemented")
//  def mulMV(x: Seq[Cell], y: Seq[Cell]): Seq[Cell] = Seq() //throw new RuntimeException("Not implemented")
//
//  
//  val CellInputPattern = """(\d+)\|(\d+)\|(\d+\.\d+)\|""".r
//
//  def parseCell = (line: String) => line match {
//    case CellInputPattern(row, col, value) => OtherCell(row.toInt, col.toInt, value.toDouble): Cell
//  }
//
//  def formatCell = (cell: Cell) => "%d|%d|%.2f|".format(cell.row, cell.col, cell.value)
//}
//
