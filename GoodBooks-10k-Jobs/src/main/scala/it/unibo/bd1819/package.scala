package it.unibo

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}

package object bd1819 {

  /**
    * Function that creates a new SQLContext
    * @return a new instance of SQLContext
    */
  //def getSqlSparkContext = sqlContext

  /**
    * Function used to load a csv file from hadoop directory in to a DataFrame object
    * @param file path to the file
    * @return DataFrame
    */
  def loadCsvfile(file: String, sqlContext: SQLContext): sql.DataFrame = {
    sqlContext.read.format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .load(file)
  }

  /**
    * Function used to slip authors from books that have more than one author
    * @param line String with authors divided by ','
    * @return List of authors
    */
  def splitAuthors(line:String): List[String] = {
    if(line.contains(','))
      line.split(',').toList
    else List(line)
  }

  /**
    * Function used to create a temporary view of a table
    * @param data DataFrame to use as a src
    * @param tableName Name of the view
    */
  def createTmpViewTable(data: DataFrame, tableName: String)= {
    data.createOrReplaceTempView(tableName)
  }


  /**
    * Return the path to books.csv file
    * @return
    */
  def pathToBooks = "./exam/dataset/books.csv"

  /**
    * Return the path to ratings.csv file
    * @return
    */
  def pathToRating = "./exam/dataset/ratings.csv"

  /**
    * Return the path to to_read.csv file
    * @return
    */
  def pathToBookmarks = "./exam/dataset/to_read.csv"

}
