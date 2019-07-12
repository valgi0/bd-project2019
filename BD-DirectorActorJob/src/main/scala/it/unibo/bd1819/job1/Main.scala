package it.unibo.bd1819.job1

import it.unibo.bd1819.{createTmpViewTable, getSqlSparkContext, loadCsvfile, splitAuthors}

object Main{

  def main(args: Array[String]):Unit = {
    val sqlcontext = getSqlSparkContext
    // First of all we need to load the file containing the books
    val booksDF = loadCsvfile("bd-project2019-master/dataset/books.csv")

    // Now we must select just two cols we need. Book id and Authors
    val ida = booksDF.select("book_id", "authors")

    // However some books cat be witten by more authors. We need to split them
    val tmp = booksDF.select("authors").flatMap(row => splitAuthors(row.getString(1)))

    // Let's create a new table
    val view = createTmpViewTable(tmp.toDF(), "tmpTable")

    //booksDF.sqlContext.sql("select authors, count(book_id) from tabTMP group by authors").show  // raggruppa per ogni autore il numero di libri scritti
    //val authorBooks = sqlcontext.sql("select authors, count(book_id) from tabTable group by authors")

    //We can now load a rating table
    val ratingDF = loadCsvfile("bd-project2019-master/dataset/rating.csv")

    // And we can create a new view
    createTmpViewTable(ratingDF, "ratingView")

    //now we lauch a sql query to get books_id and its rating average
    val idavgrating = sqlcontext.sql("select book_id, avg(rating) as average_rating from ratingView group by book_id order by average_rating desc") // calcola per ogni id la media dei rating

    val jointables = booksDF.select("book_id", "authors").join(idavgrating, "book_id")

    createTmpViewTable(jointables.toDF(), "finalview")

    val finalresult = sqlcontext.sql("select  _2, avg(_3) as avg from finalview group by _2 order by avg desc").limit(500)

    finalresult.show()
    //val uniqueAuthor = tmp.dropDuplicates
  }
}
