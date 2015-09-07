package com.soundcloud.lsh

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.scalatest.{FunSuite, Matchers}

class NearestNeighboursTest
  extends FunSuite
  with SparkLocalContext
  with Matchers {

  def denseVector(input: Double*): Vector = {
    Vectors.dense(input.toArray)
  }

  test("nearest neighbours cosine") {
    val vecA = denseVector(1.0, 0.0)
    val vecB = denseVector(0.0, 1.0)
    val vecC = denseVector(-1.0, 0.0)
    val vecD = denseVector(1.0, 0.0)

    val rows = Seq(
      IndexedRow(0, vecA),
      IndexedRow(1, vecB),
      IndexedRow(2, vecC),
      IndexedRow(3, vecD)
    )
    val indexedMatrix = new IndexedRowMatrix(sc.parallelize(rows))

    val nearestNeighbour = new NearestNeighbours(Cosine, 0.0, 1.0)
    val got = nearestNeighbour.join(indexedMatrix)

    val expected = Seq(
      MatrixEntry(0, 1, 0.0),
      MatrixEntry(0, 3, 1.0),
      MatrixEntry(1, 2, 0.0),
      MatrixEntry(1, 3, 0.0)
    )
    val gotEntries = got.entries.collect().toSeq
    gotEntries should be(expected)
  }


}
