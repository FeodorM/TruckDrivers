package ru.vsu.amm.atconsulting

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    if (args.length < 3) return

    val driversAndMiles = sc.textFile(args(1))
      .map(line => {
        val words = line split ","
        val miles = words.slice(3, 16)
          .foldLeft((0, true)) {
            case ((acc, true), nextVal) => (acc + nextVal.toInt, false)
            case ((acc, false), _) => (acc, true)
          }._1
        (words(0), miles)
      })

    val milesTable = sc.broadcast(driversAndMiles.collectAsMap)

    sc.textFile(args(0))
      .filter(!_.contains("normal"))
      .map(line => {
        val words = line split ","
        (words(1), words(1))
      })
      .aggregateByKey(Driver(""))({ case (Driver(_, num, _), id) =>
        Driver(id, num + 1, milesTable.value.getOrElse(id, -99))
      }, { case (Driver(id, num1, miles), Driver(_, num2, _)) =>
        Driver(id, num1 + num2, miles)
      })
      .map({ case (id, Driver(_, numberOfAcc, miles)) =>
        (id, numberOfAcc, miles, miles / numberOfAcc)
      })
      .saveAsTextFile(args(2))

    sc.stop()
  }
}
