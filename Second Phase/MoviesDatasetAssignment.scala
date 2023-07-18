package com.sundogsoftware.spark.MyCode

import org.apache.spark._
import org.apache.log4j._


import scala.math._


object MoviesDatasetAssignment {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "AverageRuntime")
    val dataset = sc.textFile("data/movies.data")

    // Runtime for each movie
    val data = dataset.map { line =>
      val data = line.split('\t')
      val movie_name = data.head
      val runtime = data.last
      (movie_name, runtime)
    }
    for (line <- data){
      println(line)
    }

    // Average Runtime for all movies in the dataset
    val runtime = dataset.map(x => x.split('\t')(14))
    val runtime_integer = runtime.map(_.toInt)
    val sum: Double = runtime_integer.reduce(_+_)
    println(f"The summation of all runtime is ${sum}")

    val result:Double = runtime.count()

    val averageRuntime: Double = sum / result
    println(result)
    println(f"The average run time for all movies in the dataset is ${averageRuntime}")

    // How many times each actor has benn top-billed (starred) in a movie?
    val actor = dataset.map(x=> x.split('\t')(9))
    val key_value=actor.map(actor=> (actor,1))
    val star_result = key_value.reduceByKey(_+_)
    for (row <- star_result){
      println(row)
    }

    // Average number of IMDb votes on R, PG, PG-13, G rated movies
    val averageIMDb = dataset.map{line =>
      val fields = line.split('\t')
      val rating = fields(1)
      val voting = fields(6).toLong
      (rating, voting)
    }

    // Extract R Rated movies from the dataset
    val R_Rated_Data = averageIMDb.filter(tuple => (tuple._1 == "R"))
    val R_Count = R_Rated_Data.count()
    val R_Sum = R_Rated_Data.reduceByKey(_+_)
    val R_Average = R_Sum.mapValues(value => value / R_Count)
    for (line <- R_Average){
      println(line)
    }
    // Extract PG Rated movies from the dataset
    val PG_Rated_Data = averageIMDb.filter(tuple => (tuple._1 == "PG"))
    val PG_Count = averageIMDb.count()
    val PG_Sum = PG_Rated_Data.reduceByKey(_+_)
    val PG_Average = PG_Sum.mapValues(value => value / PG_Count)
    for (result <- PG_Average){
      println(result)
    }
    // Extract G Rated movies from the dataset
    val G_Rated_Data = averageIMDb.filter(tuple => tuple._1 == "G")
    val G_Count = G_Rated_Data.count()
    val G_Sum = G_Rated_Data.reduceByKey(_+_)
    val G_Average = G_Sum.mapValues(value => value / G_Count)
    val G_Results = G_Average.collect()
    for (result <- G_Results){
      println(result)
    }
    // Extract PG13 Rated movies from the dataset
    val PG13_Rated_Data = averageIMDb.filter(tuple => tuple._1 == "PG-13")
    val PG13_Count = PG13_Rated_Data.count()
    val PG13_Sum = PG13_Rated_Data.reduceByKey(_+_)
    val PG13_Average = PG13_Sum.mapValues(value => value / PG13_Count)
    val PG13_Result = PG13_Average.collect()
    for (result <- PG13_Result){
      println(result)
    }
    // The average IMDb score of genres that have more than 9 movies
    // Extract Genre column and count how many times each movie has been scored
    val GenreColumn = dataset.map(line => line.split('\t')(2))
    val GenreMapped = GenreColumn.map(line => (line,1))
    val GenreReduced = GenreMapped.reduceByKey(_+_)
    for (result <- GenreReduced){
      println(result)
    }
    // What movies has been scored more than 9 times?
    val GenreFiltered = GenreReduced.filter(tuple => tuple._2 > 9)
    println("Movies which has been repeated more than 9 times")
    for (result <- GenreFiltered){
      println(result)
    }

    val genre = GenreFiltered.map{case (col1, _) => col1}.collect()
    val GenreRDD = sc.parallelize(genre) //Convert an Array of String to RDD

    val Score = GenreFiltered.map{case (_, col2) => col2}.collect()
    val ScoreRDD = sc.parallelize(Score)

    val ScoreGenreData = GenreRDD.zip(ScoreRDD) //Combine two RDDs



      println("After Removing the movies that have not been scored more than 9 times")
     val RecordCount = ScoreGenreData.count() // returns the number of record in the scoreGenreData RDD
     val SumOfScores = ScoreGenreData.reduceByKey(_+_)
     val GenreAverage = SumOfScores.mapValues(value => value / RecordCount)
     for (result <- GenreAverage){
      println(result)
    }


  }


}
