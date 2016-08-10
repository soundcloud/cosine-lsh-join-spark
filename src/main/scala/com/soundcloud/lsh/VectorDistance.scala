package com.soundcloud.lsh

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.mllib.linalg.Vector

/**
 * interface defining similarity measurement between 2 vectors
 */
trait VectorDistance extends Serializable {
  def apply(vecA: Vector, vecB: Vector): Double
}

/**
 * implementation of [[VectorDistance]] that computes cosine similarity
 * between two vectors
 */
object Cosine extends VectorDistance {

  def apply(vecA: Vector, vecB: Vector): Double = {
    val v1 = vecA.toArray.map(_.toFloat)
    val v2 = vecB.toArray.map(_.toFloat)
    apply(v1, v2)
  }

  def apply(vecA: Array[Float], vecB: Array[Float]): Double = {
    val n = vecA.length
    val norm1 = blas.snrm2(n, vecA, 1)
    val norm2 = blas.snrm2(n, vecB, 1)
    if (norm1 == 0 || norm2 == 0) return 0.0
    blas.sdot(n, vecA, 1, vecB, 1) / norm1 / norm2
  }

}

