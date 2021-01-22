package homework2

import org.apache.spark.sql.SparkSession

object DataApiHomeWorkTaxi extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"


  val taxiFactsDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiFactsDF.printSchema()
  println(taxiFactsDF.count())


  /**
   * Задание написать код, который будет делать следующее:
   *
   * 1. DataFrame: Какие районы самые популярные для заказов? Результат в Parquet.
   * 2. RDD: В какое время происходит больше всего вызовов? Результат в txt файл c пробелами.
   * 3. DataSet: Как происходит распределение заказов? Результат записать в базу данных Postgres.
   */

}

