package com.github.sabinesc.IMDbData

import org.apache.spark.sql.DataFrame

object DataTransform {
  def worstMovies(movies:DataFrame):DataFrame={
    movies.filter("averageRating < 2")
  }
  def bestMovies(movies:DataFrame):DataFrame = {
    movies.filter("averageRating > 9")
  }

}
