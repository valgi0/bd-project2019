package it.unibo.bd1819

import it.unibo.bd1819.job1.MainJob1
import it.unibo.bd1819.job2.{Configuration, MainJob2}

object Main {

  final private val JOB1 = "JOB1"
  final private val JOB2 = "JOB2"

  def main(args: List[String]) = {
    if (args.size != 2 && args.size != 4){
      println("USAGE: ./director-actor-job-2.1.2-spark.jar <JOB1 | JOB2>  [EXECUTORS TASKS]")
    }else{
      if(args.size == 4) {
        val conf = new Configuration(args)
        if (args(1) == JOB1) {
          MainJob1.executeJob(conf)
        } else {
          MainJob2.executeJob(conf)
        }
      }else{

      }
      if (args(1) == JOB1) {
        MainJob1.executeJob()
      } else {
        MainJob2.executeJob()
      }
    }
  }

}
