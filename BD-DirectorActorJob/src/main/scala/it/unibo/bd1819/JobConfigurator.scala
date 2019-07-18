package it.unibo.bd1819

import it.unibo.bd1819.job2.Configuration
import org.apache.spark.sql.SQLContext

class JobConfigurator {

  var sqlcontext : SQLContext = _

  def setSqlContext(sqlc: SQLContext): Unit = {
    sqlcontext = sqlc
  }


  /**
    * Set the number of parallelisms to use
    * @param executors Number of partitions
    * @param tasks Number of tasks per executor
    */
  def setParallelism(parallelism: Int) = {
    sqlcontext.setConf("spark.default.parallelism", (parallelism).toString)
  }

  /**
    * Set the number of partitions
    * @param partitions
    */
  def setPartitions(partitions: Int): Unit={
    sqlcontext.setConf("spark.sql.shuffle.partitions", partitions.toString);
  }


  def setMemoryOffHeap(memory: Int): Unit = {
    sqlcontext.setConf("spark.executor.memory", memory.toString + "g")
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
    jc.setPartitions(conf.partitions)
    jc.setParallelism(conf.parallelism)
    jc.setMemoryOffHeap(conf.memorySize)
    jc
  }

  def getDefault(context: SQLContext) : JobConfigurator = {
    val jc = new JobConfigurator()
    jc.setSqlContext(context)
    jc.setParallelism(8)
    jc.setPartitions(80)
    jc.setMemoryOffHeap(11)
    jc
  }
}
