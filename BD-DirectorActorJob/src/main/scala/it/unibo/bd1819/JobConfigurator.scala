package it.unibo.bd1819

import it.unibo.bd1819.job2.Configuration
import org.apache.spark.sql.SQLContext

class JobConfigurator {

  var sqlcontext : SQLContext = _

  def setSqlContext(sqlc: SQLContext): Unit = {
    sqlcontext = sqlc
  }

  /**
    * Set the number of partition to use
    * @param executors Number of executors
    * @param tasks Number of tasks per executor
    */
  def setPartitions(executors: Int, tasks : Int) = {
    sqlcontext.setConf("spark.sql.shuffle.partitions", (executors*tasks).toString)
  }

  /**
    * Set the number of parallelisms to use
    * @param executors Number of executors
    * @param tasks Number of tasks per executor
    */
  def setParallelism(executors: Int, tasks : Int) = {
    sqlcontext.setConf("spark.default.parallelism", (executors*tasks).toString)
  }

  /**
    * Return a SQLContext set
    * @return
    */
  def getSetSqlContext: SQLContext = sqlcontext

}


object JobConfigurator{
  var executors = 2
  var taskForExceutor = 4

  def apply(context: SQLContext) : JobConfigurator = {
    val jc = new JobConfigurator()
    jc.setSqlContext(context)
    jc
  }

  def apply(context: SQLContext, conf : Configuration): JobConfigurator = {
    val jc = new JobConfigurator()
    jc.setSqlContext(context)
    jc.setPartitions(conf.executors, conf.tasksPerExecutor)
    jc.setParallelism(conf.executors, conf.tasksPerExecutor)
    jc
  }

  def getDefault(context: SQLContext) : JobConfigurator = {
    val jc = new JobConfigurator()
    jc.setSqlContext(context)
    jc.setParallelism(executors, taskForExceutor)
    jc.setPartitions(executors, taskForExceutor)
    jc
  }
}
