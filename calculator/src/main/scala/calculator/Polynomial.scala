package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    Signal {
      val aVal: Double = a()
      val bVal: Double = b()
      val cVal: Double = c()
      bVal * bVal - 4.0 * aVal * cVal
    }
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {
    Signal {
      val aVal: Double = a()
      val bVal: Double = b()
      val dVal = computeDelta(a,b,c)()
      if (dVal < 0.0) {
        Set.empty
      } else {
        val dRoot: Double = Math.pow(dVal,0.5)
        Set(dRoot, -1.0 * dRoot).map { r => (r - bVal) / (2.0 * aVal) }
      }
    }
  }
}
