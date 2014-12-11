package us.marek.pdf.inputformat

import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.mapreduce.{ InputSplit, RecordReader, TaskAttemptContext }
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.util.PDFTextStripper

class PdfRecordReader extends RecordReader[LongWritable, Text] {

  private[this] val done = new FlipOnce(true)
  private[this] val key: LongWritable = new LongWritable(1L) //null
  private[this] val value: Text = new Text()

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit =

    inputSplit match {

      case split: FileSplit => {

        val job = context.getConfiguration
        val file = split.getPath
        val fs = file.getFileSystem(job)
        val fileIn = fs.open(split.getPath)
        val pdf = PDDocument.load(fileIn)
        val parsedText = new PDFTextStripper().getText(pdf)
        /* Sadly we have mutability, but this whole split betwee initialize() and getCurrentValue()
           comes from Hadoop Java-land :(
         */
        value.set(parsedText)
      }

      case other => throw new IllegalArgumentException(
        s"inputSplit was of type ${other.getClass.getName} but FileSplit was expected"
      )
    }

  override def nextKeyValue: Boolean = done.state

  override def getCurrentKey: LongWritable = key

  override def getCurrentValue: Text = value

  override def getProgress: Float = 0.0F

  override def close = {}

}

