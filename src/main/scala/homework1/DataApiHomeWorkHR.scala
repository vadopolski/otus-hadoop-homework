package homework1

import org.apache.spark.sql.SparkSession

object DataApiHomeWorkHR extends App {

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
   * 1. DataFrame построить витрину с 10-ю самыми оплачиваемыми должностями в компании. Результат в Parquet.
   * 2. DataSet построить витрину с сотрудниками зарплата которых выше среднего. Результат в txt файл c пробелами.
   * 3. RDD рассчитать среднюю зарплату у всех сотрудников за все все время. Результат записать в базу данных Postgres.
   */




}

