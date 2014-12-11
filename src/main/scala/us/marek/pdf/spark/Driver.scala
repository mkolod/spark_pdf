package us.marek.pdf.spark

import org.apache.hadoop.io.LongWritable
import org.apache.spark.{ SparkContext, SparkConf }
import us.marek.pdf.inputformat.{ TikaParsedPdfWritable, PdfInputFormat }

object Driver {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("PDF Parsing App")
    val sc = new SparkContext(conf)

    val pdfs = sc.newAPIHadoopFile[LongWritable, TikaParsedPdfWritable, PdfInputFormat](
      "src/test/resources/*.pdf"
    )

    pdfs.foreach {
      case (docNum, writable) =>

        println(s"Document # $docNum")
        val pdf = writable.get()
        val text = pdf.contentHandler.toString
        val metadata = pdf.metadata
        println(
          s"""
           Text:
           $text

           Metadata:
           $metadata
         """.stripMargin
        )

    }
    println(s"Number of lines in PDF: ${pdfs.count()}")
    sc.stop()
  }

}
