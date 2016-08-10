package com.soundcloud

import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.scalactic.Equality

trait TestHelper {
  /**
    * Helper class to test for equality of matrix entries using a tolerance
    * on the values.
    * @param tolerance the maximum absolute value two matrix entries are allowed
    *                  to differ in their values
    */
  class MatrixEquality(tolerance: Double) extends Equality[Array[MatrixEntry]] {

    override def areEqual(a: Array[MatrixEntry], b: Any) = {
      b match {
        case brr: Array[MatrixEntry] => a.zip(brr).map{case (a,b) => isEqual(a,b)}.foldLeft(true)(_ && _)
        case _ => true
      }
    }

    def isEqual(a: MatrixEntry, b: MatrixEntry) = a.i == b.i && a.j == b.j && math.abs(a.value - b.value) <= tolerance
  }

}
