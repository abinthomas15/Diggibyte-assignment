import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession


object Assesment extends App {
  
  case class Records(Customer_name:String,Customer_id:String,Open_date:String,Consult_Dt:String,VAC_ID:String,DR_NAME:String,State:String,Country:String,DOB:String,FLAG:String)
  
  def mapper(line:String): Records = {
    val fields = line.split('|')
    
    val records:Records = Records ( fields(0), fields(1),fields(2),fields(3),fields(4),fields(5),fields(6),fields(7),fields(8),fields(9))
    
    return records
  }
  
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first application")
  sparkConf.set("spark.master", "local[*]")
  
  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
    
  
  val myList = List("Alex|123457|20101012|20121013|MVD|Paul|SA|USA|06031987|A",
      "John|123458|20101012|20121013|MVD|Paul|TN|IND|06031987|A",
      "Mathew|123459|20121013|20121013|MVD|Paul|WAS|PHIL|06031987|A",
      "Matt|12345|20101012|20121013|MVD|Paul|BOS|NYC|06031987|A",
      "Jacob|1256|20101012|20121013|MVD|Paul|VIC|AU|06031987|A")
      
  import spark.implicits._
      
  val rdd1 = spark.sparkContext.parallelize(myList)
  
  val rdd2 = rdd1.map(mapper)
  
  val df1 = rdd2.toDF()
  
  df1.write
  .format("csv")
  .partitionBy("Country")
  .mode(SaveMode.Overwrite)
  .option("path","D:/Assignment/newfolder")
  .save()
}