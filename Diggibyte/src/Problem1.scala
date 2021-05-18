import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Problem1 extends App {
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  case class Streams(dt:String,time:String,device_name:String,house_number:String,user_id:String,country_code:String,program_title:String,season:String,season_episode:String,genre:String,product_type:String)
  
  def mapper(line:String): Streams = {
    val fields = line.split(';')
    
    val streams:Streams = Streams(fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8), fields(9), fields(10))
    
    return streams
  }
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
    
  val data = spark.sparkContext.textFile("D:/Assignment/started_streams.csv")
  
  val header = data.first
  
  val rows = data.filter(x => x != header)
    
  val rdd2 = rows.map(mapper)
  
  import spark.implicits._
  
  val dataFrame1 = rdd2.toDF
  
  val dataFrame2 = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .option("path","D:/Assignment/whatson.csv")
  .load
  
  val joinCondition = dataFrame1.col("house_number") === dataFrame2.col("house_number")
  
  val joinType = "inner"
  
   val joinDF = dataFrame1.join(dataFrame2, joinCondition, joinType).drop(dataFrame2.col("house_number")).drop(dataFrame1.col("dt"))
   .select("dt","time","device_name","house_number","user_id","country_code","program_title","season","season_episode","genre","product_type","broadcast_right_start_date","broadcast_right_end_date")
   
   joinDF.createOrReplaceTempView("data")
   
   val resultsDF = spark.sql("select * from data where product_type = 'tvod' or product_type = 'est' order by dt").show()
   
//   resultsDF.write
//   .format("csv")
//   .mode(SaveMode.Overwrite)
//   .option("path","D:/Assignment")
//   .save()
   
//  val resultsRDD = resultsDF.toJavaRDD
//  
//  resultsRDD.saveAsTextFile("D:/Assignment/newfolder2")
   
   
   
   
}