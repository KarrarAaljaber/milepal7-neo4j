package org.example

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.io.StdIn._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkNeo extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("gruppe2")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val dfContinents = spark.read.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://localhost:7687")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", "student")
    .option("labels", "Continent")
    .load()

  val dfCountry = spark.read.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://localhost:7687")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", "student")
    .option("labels", "Country")
    .load()

  var stopped = false
  println("hey there, using this program you can view and add countries to the neo4j database")
  var command = 0


  var continents = dfContinents.select("name").collectAsList()
  var countries = dfContinents.select("countries").collectAsList()

  while(!stopped){
    println("type 1 to add a new country, 2 to view all the countries in the database, 3 to sort by GDP per capita"
      + " and 4 to quit the program")
    command = readInt()
    if(command==1){
      val country = readLine("Enter the country name: ")
      print("Enter the GDP per Capita for this country:")
      val income = readInt()
      println("Which continent is this country located in?(enter a number to choose one)")
      var c =0
      for(c <- 0 to continents.size() - 1){
          println( c + 1 + " : " + continents.get(c))
      }
      val cNum = readInt()
      if(cNum < 1 || cNum > 5){
        println("The number you entered dosent correspond to any of the continents.... quitting program")
        stopped = true
      }


      //country node
      val schema = StructType(Array(
        StructField("name",StringType,true),
        StructField("GdpPerCapita",IntegerType,true)
      ))
      val temp = Seq(Row(country,income))

      val dfForCountry = spark.createDataFrame(
        spark.sparkContext.parallelize(temp),schema)

      dfForCountry.write.format("org.neo4j.spark.DataSource")
        .mode(SaveMode.ErrorIfExists)
        .option("url", "bolt://localhost:7687")
        .option("authentication.basic.username", "neo4j")
        .option("authentication.basic.password", "student")
        .option("labels", "Country")
        .save()
      /*
      val schemaForContinent = StructType(Array(
        StructField("name",StringType,true),
        StructField("countries",StringType,true)
      ))

      val dd = StructType(
        Array(
          StructField("name", StringType, true),
          StructField("countries", ArrayType(StringType), true)
        )
      )

      val con = Seq(Row(continents.get(cNum),countries))

      val dfForContinent = spark.createDataFrame(
        spark.sparkContext.parallelize(con),dd)

      dfForContinent.write.format("org.neo4j.spark.DataSource")
        .mode(SaveMode.Overwrite)
        .option("url", "bolt://localhost:7687")
        .option("authentication.basic.username", "neo4j")
        .option("authentication.basic.password", "student")
        .option("labels", "Continent")
        .save()

    */


    }else if(command==2){
     dfCountry.show(dfCountry.count().toInt,false)

    }else if(command==3){

      println("sorted by GDP per Capita")
      val gdpTop = dfCountry.sort(col("GdpPerCapita").desc).show()

    }

    else if(command == 4){
      stopped = true
      println("quiting program....")
    }else{
      println("you typed a wrong command, try again")

    }

  }







}
