package it.unibo.bd1819

import it.unibo.bd1819.job1.MainJob1
import it.unibo.bd1819.job2.{Configuration, MainJob2}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Main{

  final private val JOB1 = "JOB1"
  final private val JOB2 = "JOB2"

  def main(args: Array[String]): Unit = {
    if (args.length != 1 && args.length != 4){
      println("USAGE: ./director-actor-job-2.1.2-spark.jar <JOB1 | JOB2>  [PARTITIONS PARALLELISM MEMORY]")
      println("Found: " + args.length)
    }else{
      val sqlcontext = SparkSession.builder.master("local[*]").getOrCreate.sqlContext
      if(args.length == 4) {
        val conf = Configuration(args.toList)
        if (args(0) == JOB1) {
          MainJob1.apply.executeJob(conf, sqlcontext)
        } else {
          MainJob2.apply.executeJob(conf, sqlcontext)
        }
      }else{
        if (args(0) == JOB1) {
          MainJob1.apply.executeJob(Configuration(), sqlcontext)
        } else {
          MainJob2.apply.executeJob(Configuration(), sqlcontext)
        }
      }

    }
  }

}
