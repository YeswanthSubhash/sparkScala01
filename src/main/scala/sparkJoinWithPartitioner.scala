import org.apache.spark.HashPartitioner
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object sparkJoinWithPartitioner {

  def main(args:Array[String])={

  val spark=SparkSession.builder().appName("JoinWithPartitioner").master("local[*]").getOrCreate()
   spark.conf.set("spark.sql.autoBroadcastJoin","-1")
   spark.conf.set("spark.sql.preferSortMergeJoin",false)
   spark.conf.set("spark.sql.shuffle.partition",2)
    val hp=new HashPartitioner(2)
    import spark.implicits._
    val smalldf=Seq.range(1,5).toDF("id").withColumn("randoms",rand)
    val largedf=Seq.range(1,10).toDF("id").withColumn("randoml",rand)
    import spark.implicits._
    val smallrdd=smalldf.rdd.map(x=>(x.getInt(0),x)).partitionBy(hp).values.persist()
    val largerdd=largedf.rdd.map(x=>(x.getInt(0),x)).partitionBy(hp).values.persist()
    //smallrdd.foreach(println)
    val joindf=largerdd.join(smallrdd,Seq("id"))
    print(smallrdd.partitioner)







  }

}
