import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import java.util.Properties
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ ForeachWriter, Row }
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import scala.collection.mutable
import org.apache.spark.sql.functions._
import java.io.File
import scala.collection.mutable
import java.sql.DriverManager
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.streaming.Trigger

object Spark_copy {
  def main( args: Array[ String ] ) {
    
    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    val spark = SparkSession.builder().master("local[1]").appName( "Spark_copy" ).getOrCreate()
    import spark.implicits._
    
    
    val loan_schema = spark.read.format("csv").load("/path to csv/LoanStats_2018Q4.csv").schema //Path to CSV file to read the schema
    
    val stream_df = spark.readStream
			  .format( "csv" )
    			  .option("header", "true")
    			  .schema(loan_schema)
    			  .load("/path to csv/") //Directory path to read the data
                         .select(
                           col( "_c2" ).as( "loan_amount" ),
                           col( "_c3" ).as( "funded_amount" ),
                           col( "_c12" ).as( "home_ownership" ),
                           col( "_c15" ).as( "verfication_status" ),
                           col( "_c16" ).as( "issue_date" ) )
      
    val jdbcUrl = "jdbc:postgresql://localhost/dataengineering"
    val dbUser = "postgres"
    val dbPass = "*****" //Mention the password to connect to DB
    val table = "loan_data"
    val driver = "org.postgresql.Driver"
    
    
class JDBCSink( jdbcUrl: String, user:String, pwd:String, driver: String,batchSize: Int ) extends org.apache.spark.sql.ForeachWriter[ org.apache.spark.sql.Row ] {
 
    var connection:java.sql.Connection = _
    var statement:java.sql.Statement = _
    var copyManager: CopyManager = _
    var sb = new StringBuilder
    var rowsProcessed = 0
    sb.ensureCapacity( batchSize*30 )
    

    override def open( partitionId: Long, version: Long ):Boolean = {
      Class.forName( driver )
      connection = java.sql.DriverManager.getConnection( jdbcUrl, user, pwd )
      connection.setAutoCommit( true )
      copyManager = new CopyManager( connection.asInstanceOf[ BaseConnection ] )
  
      true
    }

    override def process( value: org.apache.spark.sql.Row ): Unit = {
    
      convertRowtoString( value )
      rowsProcessed += 1
      if ( rowsProcessed % batchSize == 0 ) {
        commitMsg()
      }
    
    }

    override def close( errorOrNull:Throwable ):Unit = {

     commitMsg()
     connection.close
    }
    
    def convertRowtoString ( rows : Row ) : StringBuilder = {
      val row = rows.mkString( "," )
      sb.append( row ).append( "\n" )
    
    }
    
    def commitMsg( ) : Unit = {
      val rowBytes = sb.toString().getBytes
      val sql = "COPY " + table + """ FROM STDIN WITH (NULL '\N', DELIMITER ',')""" 
      val copyIn = copyManager.copyIn( sql )
      copyIn.writeToCopy( rowBytes, 0, rowBytes.length )
      copyIn.endCopy()
      sb.clear()
      rowsProcessed = 0
    }
}       
                      
                      
    stream_df.printSchema()
                              
    val writer = new JDBCSink( jdbcUrl, dbUser, dbPass, driver, batchSize = 1000 )
    
    stream_df.writeStream
             .foreach( writer )
             .outputMode( "append" )
             .start()
             .awaitTermination()
    }
}
