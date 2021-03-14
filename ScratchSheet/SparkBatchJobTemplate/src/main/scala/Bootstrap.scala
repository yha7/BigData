import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.hadoop.fs._
import org.apache.spark.sql.functions.{col,input_file_name,substring_index}
import org.apache.spark.sql.types.{StringType,StructField,StringType}

object Bootstrap extends App{

  //creating spark session
  val spark = SparkSession
    .builder()
    .appName("appRecursiveHdfsProcessor")
    .config("spark.master", "local[1]")
    .getOrCreate()
  //creating spark context
  val sc=spark.sparkContext
  val hdfsInputPath="../loc/..";

  def processor: Unit ={
  val path=new Path(hdfsInputPath);
    val hdfsConfiguratoion=sc.hadoopConfiguration
  val listOfFilesInPath=FileSystem.get(hdfsConfiguratoion).listStatus(path)
    .map(x=>x.getPath.toString).toList

    val expectedColumnList="somelist";
    listOfFilesInPath.foreach(fileInPath =>{


      val df= spark.read.format("csv")
        .option("delimiter",";")
        .option("header","true")
        .load(fileInPath)

      println("schema : "+df.printSchema())
      val actualColumnList = df.columns.toList

      actualColumnList.foreach(println(_))

      val isAllColumnsExpectedExist = expectedColumnList.map(x=>actualColumnList.contains(x)).toList
      isAllColumnsExpectedExist.contains("false") match {
        case true => {  /*update audit table move original file to rejected folder*/ }
        case false => {  /*update audit table, write to success table move original file to processed folder*/ }
      }
    })
  }
}
