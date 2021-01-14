package logic

import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.rdd.RDD

object BootStrap extends App{

  val log = Logger.getLogger(BootStrap.getClass)
  PropertyConfigurator.configure("src/main/resources/conf/log4j.properties")

  //Creating spark session
  val spark = SparkSession
    .builder()
    .appName("app-ts-generate-sessionid")
    .config("spark.master", "local[1]")
    .getOrCreate()

  log.info("Spark session object created successfully.")

  val sc = spark.sparkContext

  //creation of rdd
  val rdd = sc.wholeTextFiles("src/main/resources/source/").map(_._2)

  //First Approach :- Create a case class which extends Anonymus Function.This extends from Function1 trait.
  // It avoids "Task Not Serializable" issue.Why? Case Class by default extends Serializable
  // and hence the issue is avoided.
  //There are 2 apply methods in this context.First apply method is the case class apply method. For ex :- Val a = Anonym().here case class
  // companion object apply method is called which returns a case class object.Second apply method comes from Function 1 trait.So we have to
  // override this to acheive our functionality.
  //Q) Why Function1 trait not Function 2 or anything else?
  //A) Map Partitions takes a function 1 as a parameter f : scala.Function1[scala.Iterator[T], scala.Iterator[U]] or a single argument.

  //Second Approach:- Make a function which accepts Iterator as a argument and return type is also Iterator.Here we may get serialization
  // issue.Need to prove this point by writing some code.Will do it later

  //Third Approach:-Make a class which extends Serializable and Anonymus Function.This extends from Function1 trait.Unlike case class a normal
  // class does not extend Serializable.Here we need to first create object of this class by "new" keyword and then we can use the object of
  // it in map partitions method. In this context we have only 1 apply method which is of Function1 trait.


  //First approach
  rdd.mapPartitions(Anonymous()).foreach(println(_))
  case class Anonymous () extends (Iterator[String] => Iterator[String])
  {
    def apply(iterator:Iterator[String]): Iterator[String] = {
      {
        iterator.map(x => "FIRST APPROACH WORKING" )
      }
    }
  }

  //Second approach
  rdd.mapPartitions( x=>anon(x)).foreach(println(_))
  def anon(x:Iterator[String]):Iterator[String]={
    x.map(x => "Second approach working")
  }

  //Third approach-with apply method of Function1 trait
  val anonym = new Anonym()
  rdd.mapPartitions(new Anonym()).foreach(println(_))

   class Anonym () extends (Iterator[String] => Iterator[String]) with Serializable
   {

     def apply(iterator:Iterator[String]): Iterator[String] = {
       {

         iterator.map(x => "Third approach WORKING -with apply method of Function1 trait" )
       }
     }
   }

  //Third approach working-without apply method of Function1 trait
     rdd.mapPartitions( new some().anon(_)).foreach(println(_))

  class some ()
  {
    val g = "Third approach working-without apply method of Function1 trait"
    def anon(x:Iterator[String]):Iterator[String]={
      x.map(x => g)
    }
  }
}
