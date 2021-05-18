import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Problem3 {
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
  
  dataFrame1.createOrReplaceTempView("data")
  
  val resultsDF = spark.sql("select genre, count(*) as unique_users from data group by genre ").show(100)
  
//  resultsDF.write
//  .format("csv")
//  .mode(saveMode.Overwrite)
//  .option("path","D:/Assignment/newfolder")
//  .save()
  
}