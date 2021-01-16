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



  /**
   * Описание источника:
   * файл с зарплатами находится в src/main/resources/salary.csv
   * выше указаны параметры подключения для базы данных с источником sql/db.sql
   *
   * Задание написать код, который будет делать следующее:
   *
   * 1. DataFrame: Какие районы самые популярные для заказов? Результат в Parquet.
   * 2. RDD: В какое время происходит больше всего вызовов? Результат в txt файл c пробелами.
   * 3. DataSet: Как происходит распределение заказов? Результат записать в базу данных Postgres.
   */

}

