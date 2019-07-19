
# Big-Data: GoodBooks-10k-Jobs

`@ authors: ` Lorenzo Valgimigli  
`@ authors: ` Riccardo Soro  
`@ Course : ` Big Data  
`@ Data   : ` 26/07/19


## Target

The dataset is a books collection with some information as: ratings, bookmarks etc etc ...  
You find it at this github repository: 

[Good books 10k repository](https://github.com/zygmuntz/goodbooks-10k "GoodBooks repository")

## Jobs

The jobs the team had to develop are:

1. Find out the best 500 authors. In order to do that we had to compute the average rating
for each book and compute the average rating for each author.

2.  Find out a correlation between bookmarks and rating. Bookmark is the desire for a user
to read a book. 

Both are implemented using **Hadoop Map Reduce** and **Spark SQL**.

## Deploy

You can build the project using gradle or gradlew. In order to build jars you can use one of this tasks:

1. `sparkJar` to build the Spark Sql code.
2. `mapReduceJarJob1` to build the first job developed using Map Reduce
3. `mapReduceJarJob2` to build the second job developed using Map Reduce





