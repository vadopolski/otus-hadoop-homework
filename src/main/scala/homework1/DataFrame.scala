package homework1

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._


object DataFrame extends App {

  // создаем спарк сессию
  val session = SparkSession.builder()
    .appName("DataFrames")
    .config("spark.master", "local")
    .getOrCreate()


/*
  Задание 1

  Создайте в коде датафрейм с популярными курортами, содержащаю следующие поля
    - страна
    - нужна ли виза
    - средняя стоимость на неделю на двоих в $
    - как добираться

  Вывести в лог
*/


  /*
    Задание 2

    Создайте датафрейм из файла population_data.json

    Вывести в лог
      - схему
      - количество строк в файле
  */




}