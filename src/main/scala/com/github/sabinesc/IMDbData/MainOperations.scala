package com.github.sabinesc.IMDbData

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, DoubleType}

object MainOperations {
  def main(args: Array[String]): Unit = {
    if(args.length<2 || args.length>4) {
      println("Inserted number of arguments is not acceptable")
      println("Program needs 4 parameters to run:")
      println(" 1.Path for folder in which files will be downloaded from web")
      println(" 2.Path for database")
      println(" 3.Database username(empty string if it has none)")
      println(" 4.Database password(empty string if it has none)")
      sys.exit(1)
    }

  val fileFolderPath: String = args(0)
  val url: String = "https://datasets.imdbws.com/"
  val fileNameList: List[String] = List("title.basics.tsv.gz", "title.ratings.tsv.gz")
  println(s"Saving files: " + fileNameList + " in location: " + fileFolderPath)
  val filePathList: List[String] = fileNameList.map(fileFolderPath + "/" + _)

  fileNameList.map(name => FileUtilities.fileDownloader(url + name, name, fileFolderPath))
  println(s"File " + fileNameList + " saved in location: " + fileFolderPath)

    val basicInfo: DataFrame = SparkOperations.dataFrameFromTsvFile(filePathList(0))
    val ratings: DataFrame = SparkOperations.dataFrameFromTsvFile(filePathList(1))

    val joinedData:DataFrame = basicInfo.join(ratings,"tconst")

    val movies:DataFrame = joinedData
      .select(
        joinedData("titleType"),
        joinedData("primaryTitle"),
        joinedData("startYear").cast(IntegerType),
        joinedData("runtimeMinutes").cast(IntegerType),
        joinedData("averageRating").cast(DoubleType),
        joinedData("numVotes").cast(IntegerType))
      .drop("tconst","originalTitle", "isAdult", "endYear", "genres")
      .where("titleType = 'movie'")
      .orderBy(org.apache.spark.sql.functions.col("numVotes").desc)
    println("Top 10 movies with highest number of votes:")
    movies.show(10)

    SparkOperations.createDbTable(args(1), "movies", movies, args(2), args(3))

    println("10 movies with low rating (<2):")
    DataTransform.worstMovies(movies).show(10)

    println("10 movies with high rating (>9):")
    DataTransform.bestMovies(movies).show(10)

  }
}
