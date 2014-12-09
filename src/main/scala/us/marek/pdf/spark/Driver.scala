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

    val pdfLines = sc.newAPIHadoopFile[LongWritable, Text, PdfInputFormat](
      "src/main/resources/backus.pdf"
    )

    pdfLines.foreach(println)
    println(s"Number of lines in PDF: ${pdfLines.count()}")
    sc.stop()
  }

}
