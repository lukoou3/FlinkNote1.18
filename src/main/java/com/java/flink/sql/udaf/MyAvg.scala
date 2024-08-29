package com.java.flink.sql.udaf

import org.apache.flink.table.functions.{AggregateFunction, FunctionContext}
import com.java.flink.sql.udaf.MyAvg.AccData

class MyAvg extends AggregateFunction[java.lang.Double, AccData]{
  override def createAccumulator(): AccData = AccData(0, 0)


  override def open(context: FunctionContext): Unit = {
    println(context)
  }

  def accumulate(acc: AccData, value: java.lang.Double): Unit = {
    if(value != null){
      acc.sum += value
      acc.count += 1
      println(acc)
    }
  }

  def merge(acc: AccData, it: java.lang.Iterable[AccData]): Unit = {
    val iter = it.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.count += a.count
      acc.sum += a.sum
    }
  }

  override def getValue(acc: AccData): java.lang.Double = {
    if(acc.count == 0){
      null
    }else{
      acc.sum / acc.count
    }
  }

}

object MyAvg{
  case class AccData(
    var sum: Double,
    var count: Int
  )
}
