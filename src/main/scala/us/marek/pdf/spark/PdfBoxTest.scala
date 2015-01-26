package us.marek.pdf.spark

import org.apache.pdfbox.pdmodel.{ PDPage, PDDocument }
import org.apache.pdfbox.util.{TextPosition, PDFTextStripper, PDFStreamEngine}
import scala.collection.JavaConversions._
/**
 * @author Marek Kolodziej
 * @since 1/26/2015
 */
object PdfBoxTest extends App {

  val doc = PDDocument.load("/Users/mkolodziej/Downloads/pdfs/Nitro/NitroPro8UserGuide.pdf")
  val stripper = new MyPDFTextStripper()
  stripper.setStartPage(1)
  stripper.setEndPage(doc.getNumberOfPages)
  val text = stripper.getText(doc)
  val chars = stripper.myGetCharactersByArticle
  val allTextPos = chars.flatten[TextPosition]
  val groupByFont = allTextPos.groupBy(_.getFont.getBaseFont).map {
    case (font, chars) => (font, chars.size)
  }.toList.sortWith((a, b) => a._2 > b._2)

  println(groupByFont)
}


class MyPDFTextStripper extends PDFTextStripper {

  import java.util.{ List, Vector }

  def myGetCharactersByArticle: Vector[List[TextPosition]] = getCharactersByArticle
}