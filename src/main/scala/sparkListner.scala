
import org.apache.spark.sql._
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerStageCompleted, SparkListenerTaskEnd}
object sparkListner {

  def main (args:Array[String])={

    var recordsReadCount = 0L
    var recordsWrittenCount = 0L
    var reason=""
    val spark=SparkSession.builder().appName("SparkListner").master("local[*]").getOrCreate()
    val sc=spark.sparkContext
    val df=spark.read.option("header","true").csv("file://"+getClass.getResource("test.csv").getPath)


    class customSparkListener extends SparkListener  {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
        synchronized {
          reason=taskEnd.reason.toString
          recordsReadCount += taskEnd.taskMetrics.inputMetrics.recordsRead
          recordsWrittenCount+=taskEnd.taskMetrics.outputMetrics.recordsWritten


        }
      }
    }

    val myListner=new customSparkListener()
    sc.addSparkListener(myListner)
    df.write.csv("file:///C://Users//Midhun//Documents//result")


    println(s"Records Read Count: $recordsReadCount")
    println(s"Records Writes Count: $recordsWrittenCount")
    println(s"Task Status: $reason")





  }

}
