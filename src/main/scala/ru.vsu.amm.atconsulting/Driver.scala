package ru.vsu.amm.atconsulting

case class Driver(driverId: String, numberOfAccidents: Int = 0, miles: Int = -99) {
  def riskFactor(): Int =
    if (numberOfAccidents != 0) miles / numberOfAccidents else -1
}
