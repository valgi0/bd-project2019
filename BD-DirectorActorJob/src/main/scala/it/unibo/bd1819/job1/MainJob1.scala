package it.unibo.bd1819.job1

import it.unibo.bd1819._
import it.unibo.bd1819.job2.Configuration
import org.apache.spark.sql.{Encoders, SQLContext}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class MainJob1{

  val schema = StructType(Seq(
    StructField("book_id", StringType),
    StructField("Authors", StringType)
  ))
  val encoder = Encoders.STRING

  var sqlcontext : SQLContext = _

  def executeJob(conf: Configuration, sqlc: SQLContext):Unit = {

    // If users has not specified partitions and tasks for each partitions jobs use default
    if(conf.partitions == 0){
      sqlcontext = JobConfigurator.getDefault(sqlc).getSetSqlContext
    }else{
      sqlcontext = JobConfigurator(sqlc, conf).getSetSqlContext
    }


    // First of all we need to load the file containing the books
    val booksDF = loadCsvfile(pathToBooks,sqlcontext)
    println("[DEBUG] booksDF created. \n\tSchema: " + booksDF.printSchema())

    // Now we must select just two cols we need. Book id and Authors
    val ida = booksDF.select("book_id", "authors")
    println("[DEBUG] ida (ID-AUTHOR) created. \n\tSchema: " + ida.printSchema())

    // However some books cat be witten by more authors. We need to split them
    val tmp = booksDF.select("authors").flatMap(row => splitAuthors(row.getString(1)))(encoder)
    println("[DEBUG] tmp created. \n\tSchema: " + tmp.toDF().printSchema())

    // Let's create a new table
    val view = createTmpViewTable(tmp.toDF(), "tmpTable")

    //booksDF.sqlContext.sql("select authors, count(book_id) from tabTMP group by authors").show  // raggruppa per ogni autore il numero di libri scritti
    //val authorBooks = sqlcontext.sql("select authors, count(book_id) from tabTable group by authors")

    //We can now load a rating table
    val ratingDF = loadCsvfile(pathToRating, sqlcontext)

    // And we can create a new view
    createTmpViewTable(ratingDF, "ratingView")

    //now we lauch a sql query to get books_id and its rating average
    val idavgrating = sqlcontext.sql("select book_id, avg(rating) as average_rating from ratingView group by book_id order by average_rating desc") // calcola per ogni id la media dei rating

    val jointables = booksDF.select("book_id", "authors").join(idavgrating, "book_id")

    createTmpViewTable(jointables.toDF(), "finalview")

    val finalresult = sqlcontext.sql("select  authors, avg(average_rating) as avg from finalview group by authors order by avg desc").limit(500)

    finalresult.show()
    //val uniqueAuthor = tmp.dropDuplicates
  }
}

object MainJob1{
  def apply: MainJob1 = new MainJob1()
}
