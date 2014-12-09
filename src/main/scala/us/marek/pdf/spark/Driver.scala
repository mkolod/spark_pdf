package us.marek.pdf.spark

import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkContext, SparkConf }
import us.marek.pdf.inputformat.PdfInputFormat

object Driver {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("PDF Parsing App")
    val sc = new SparkContext(conf)

    val files = sc.newAPIHadoopFile[LongWritable, Text, PdfInputFormat](
      "file:///Users/marek/Downloads/lambda.pdf"
    )

    //    val files = sc.textFile("file:///Users/marek/Downloads/out.txt")

    //    files.collect().foreach(println)

    sc.stop()
  }

}
