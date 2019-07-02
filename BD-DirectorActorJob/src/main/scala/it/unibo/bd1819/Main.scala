package it.unibo.bd1819

class Main {

  // val booksDF = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("bd-project2019-master/dataset/books.csv")
  //val ida = booksDF.select("book_id", "authors")
  //def g(x:String) = if(x.contains(',')) x.split(',').toList else List(x)
  //val tmp =booksDF.select("authors").flatMap(row1 => g(row1.getString(0)))
  //booksDF.sqlContext.sql("select authors, count(book_id) from tabTMP group by authors").show  // raggruppa per ogni autore il numero di libri scritti
  // val idavgrating = rating.sqlContext.sql("select book_id, avg(rating) as average_rating from ratingview group by book_id order by average_rating desc") // calcola per ogni id la media dei rating
  // books.select("book_id", "authors").join(idavgrating, "book_id").show
  //dautmeans.sqlContext.sql("select  _2, avg(_3) as avg from finalview group by _2 order by avg desc").limit(500).show(30)
  //val uniqueAuthor = tmp.dropDuplicates
}
