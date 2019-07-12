package it.unibo.bd1819.job2
import it.unibo.bd1819.{createTmpViewTable, getSqlSparkContext,
  loadCsvfile, splitAuthors, pathToBooks, pathToBookmarks,pathToRating}

object MainJob2 extends {

  final private val rating_treshold = 3.5
  final private val bookmarks_treshold = 90

  def executeJob(conf: Configuration) = {

    val sqlcontext = getSqlSparkContext

    // Let's load the dataset
    val books = loadCsvfile(pathToBooks)
    val bookMarksFD = loadCsvfile(pathToBookmarks)
    val ratingFD = loadCsvfile(pathToRating)

    //Now we have to count foreach books how many bookmarks are registered
    createTmpViewTable(bookMarksFD, "bookmarks")
    val bookmarksForBoks = sqlcontext.sql("SELECT book_id, count(user_id) as marks FROM bookmarks "+
      "GROUP BY book_id ORDER BY marks DESC")

    // now we hav to do the same with the rating
    createTmpViewTable(ratingFD, "ratings")
    val ratingForeachBooks = sqlcontext.sql("SELECT book_id, AVG(rating) as avgRating FROM ratings" +
      "GROUP BY book_id ORDER BY avgRating DESC")

    // now we have to merge the two tables
    //createTmpViewTable(bookmarksForBoks.toDF(), "bookmarks_for_books")
    //createTmpViewTable(ratingForeachBooks.toDF(), "rating_for_books")

    val joinedTable = bookmarksForBoks.join(ratingForeachBooks, "book_id").orderBy("avgRating")

    //select all upper case
    createTmpViewTable(joinedTable.toDF(), "joinedTable")
    val coutMostBMandMostRating = sqlcontext.sql("SELECT * FROM joinedTable WHERE" +
    "avgRating >= " + rating_treshold + " AND marks >= " + bookmarks_treshold).count()

    // select all lower case

    val coutLessBMandLessRating = sqlcontext.sql("SELECT * FROM joinedTable WHERE" +
      "avgRating < " + rating_treshold + " AND marks <= " + bookmarks_treshold).count()

    // at the end
    println("----------------------------------------------------\nJob done")
    println("RESULT: ")
    println("Books with high bookmarks and high ratings are: " + coutMostBMandMostRating)
    println("Books with few bookmarks and low ratings are: " + coutLessBMandLessRating)

  }

}
