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

  val dfRelation = spark.read.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://localhost:7687")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", "student")
    .option("query", "MATCH (source:Country)-[r:LOCATED_IN]->(target:Continent) RETURN source.name, source.GdpPerCapita,target.name")
    .load()

  var stopped = false
  println("hey there, using this program you can view and add countries to the neo4j database")
  var command = 0


  var continents = dfContinents.select("name").collectAsList()

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
          println( c + " : " + continents.get(c))
      }

      val cNum = readInt()
      if(cNum < 0 || cNum > 4){
        println("The number you entered dosent correspond to any of the continents.... quitting program")
        stopped = true
      }


      val data = Seq(
        (country, income, continents.get(cNum).getString(cNum)))
      val countryDF = spark.createDataFrame(data).toDF("name", "GdpPerCapita" , "continent")

      countryDF.write
        .format("org.neo4j.spark.DataSource")
        .option("url", "bolt://localhost:7687")
        .option("authentication.basic.username", "neo4j")
        .option("authentication.basic.password", "student")
        .option("relationship", "LOCATED_IN")
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.labels", ":Country")
        .option("relationship.source.save.mode", "overwrite")
        .option("relationship.source.node.keys", "name:name")
        .option("relationship.source.node.properties", "GdpPerCapita:GdpPerCapita")
        .option("relationship.target.labels", ":Continent")
        .option("relationship.target.node.keys", "continent:name")
        .option("relationship.target.save.mode", "Overwrite")
        .save()

      countryDF.show()



    }else if(command==2){
      dfRelation.show(dfRelation.count().toInt,false)

    }else if(command==3){

      //val gdpTop = dfCountry.sort(col("GdpPerCapita").desc).show()




      println("TOP 10 (sorted by GDP per Capita)")
      val dd = spark.read.format("org.neo4j.spark.DataSource")
        .option("url", "bolt://localhost:7687")
        .option("authentication.basic.username", "neo4j")
        .option("authentication.basic.password", "student")
        .option("query", "MATCH (source:Country)-[r:LOCATED_IN]->(target:Continent)  RETURN source.name, source.GdpPerCapita,target.name ORDER BY source.GdpPerCapita DESC")
        .load()

      dd.show(10,false)

    }

    else if(command == 4){
      stopped = true
      println("quiting program....")
    }else{
      println("you typed a wrong command, try again")

    }

  }







}
