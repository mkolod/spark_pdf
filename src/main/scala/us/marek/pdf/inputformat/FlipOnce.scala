package us.marek.pdf.inputformat

/**
 * Switch that can only be flipped once.
 * E.g. start with false. The first call to state will return false,
 * but all subsequent ones will return true.
 * The reverse logic applies to the initial state being true.
 *
 * @param initial
 */
class FlipOnce(initial: Boolean) {

  import scala.reflect.runtime.{ currentMirror => cm }
  import scala.reflect.runtime.universe._

  lazy val internal: Int = 123 // all that matters is that it's a lazy val Int not equal to zero

  val im = cm reflect this
  val f  = typeOf[FlipOnce].member(newTermName("internal")).asTerm.accessed.asTerm

  def state = {
    // was the lazy field initialized?
    val status = this synchronized ((im reflectField f).get != 0)
    // flip the switch by accessing field
    if (!status) this.internal
    // return original or flipped state, depending on status (original if false, flipped if true)
    initial ^ status
  }

}