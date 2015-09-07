package com.soundcloud.lsh

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.scalatest.{FunSuite, Matchers}

class  VectorSimilarityTest
  extends FunSuite
  with SparkLocalContext
  with Matchers {

  def denseVector(input: Double*): Vector = {
    Vectors.dense(input.toArray)
  }

  test("cosine similarity") {
    val vecA = denseVector(1.0, 0.0)
    val vecB = denseVector(0.0, 1.0)
    val vecC = denseVector(-1.0, 0.0)
    val vecD = denseVector(1.0, 0.0)

    val perpendicular = Cosine(vecA, vecB)
    perpendicular should be(0.0)

    val opposite = Cosine(vecA, vecC)
    opposite should be(-1.0)

    val same = Cosine(vecA, vecD)
    same should be(1.0)
  }

  test("similarities") {
    val vec1 = Vectors.dense(1.0, 2.0, 3.0)
    val vec2 = Vectors.dense(1.0, 2.0, 4.0)
    val vec3 = Vectors.dense(7.0, 7.0, 9.0)

    Cosine(vec1, vec2) should be >= Cosine(vec1, vec3)
    Cosine(vec2, vec1) should be >= Cosine(vec2, vec3)
    Cosine(vec3, vec1) should be >= Cosine(vec3, vec2)
  }
}
