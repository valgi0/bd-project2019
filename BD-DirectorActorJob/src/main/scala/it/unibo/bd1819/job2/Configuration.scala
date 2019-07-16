package it.unibo.bd1819.job2

class Configuration {

  var executors : Int = 0
  var tasksPerExecutor: Int = 0

}



object Configuration{
  def apply(args: List[String]): Configuration = {
    var c = new Configuration()
    c.executors = args(1).toInt
    c.tasksPerExecutor = args(2).toInt
    c
  }

  def apply(): Configuration = new Configuration()

  def apply(exec : Int, task: Int): Configuration = {
    val c =new Configuration()
    c.executors = exec
    c.tasksPerExecutor = task
    c
  }
}