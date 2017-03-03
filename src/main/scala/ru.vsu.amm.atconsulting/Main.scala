package ru.vsu.amm.atconsulting

import com.sun.org.apache.xpath.internal.functions.WrongNumberArgsException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length < 3)
      throw new WrongNumberArgsException("Должно быть 3 аргумента")

    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    val milesTable = sc.broadcast(getMapWithMiles(sc, sc.textFile(args(1))))
    val drivers = getDrivers(sc, sc.textFile(args(0)), milesTable)

    prepareForOutput(sc, drivers).saveAsTextFile(args(2))

    sc.stop()
  }

  def getMapWithMiles(sc: SparkContext, rdd: RDD[String]): collection.Map[String, Int] =
    rdd.map(line => {
      val words = line split ","
      val miles = words.slice(3, 16)
        .foldLeft((0, true)) {
          case ((acc, true), nextVal) => (acc + nextVal.toInt, false)
          case ((acc, false), _) => (acc, true)
        }._1
      (words(0), miles)
    })
    .collectAsMap

  def getDrivers(sc: SparkContext, rdd: RDD[String],
                 broadcast: Broadcast[collection.Map[String, Int]]): RDD[Driver] =
    rdd.filter(!_.contains("normal"))
      .map(line => {
        val words = line split ","
        (words(1), words(1))
      })
      .aggregateByKey(Driver(""))({ case (Driver(_, num, _), id) =>
        Driver(id, num + 1, broadcast.value.getOrElse(id, -99))
      }, { case (Driver(id, num1, miles), Driver(_, num2, _)) =>
        Driver(id, num1 + num2, miles)
      })
      .map({ case (_, driver) => driver })

  def prepareForOutput(sc: SparkContext, rdd: RDD[Driver]): RDD[(String, Int, Int, Int)] =
    rdd.map({ case Driver(id, numberOfAcc, miles) =>
      (id, numberOfAcc, miles, miles / numberOfAcc)
    })
}
