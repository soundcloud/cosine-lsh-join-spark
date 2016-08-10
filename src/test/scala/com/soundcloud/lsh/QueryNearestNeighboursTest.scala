package com.soundcloud.lsh

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.scalatest.{FunSuite, Matchers}

class QueryNearestNeighboursTest
  extends FunSuite
    with SparkLocalContext
    with Matchers {

  def denseVector(input: Double*): Vector = {
    Vectors.dense(input.toArray)
  }

  test("nearest neighbours cosine") {
    val queryVectorA = denseVector(1.0, 1.0)
    val queryVectorB = denseVector(-1.0, 1.0)

    val catalogVectorA = denseVector(1.0, 1.0)
    val catalogVectorB = denseVector(-1.0, 1.0)
    val catalogVectorC = denseVector(-1.0, 0.5)
    val catalogVectorD = denseVector(1.0, 0.5)

    val queryRows = Seq(
      IndexedRow(0, queryVectorA),
      IndexedRow(1, queryVectorB)
    )

    val catalogRows = Seq(
      IndexedRow(0, catalogVectorA),
      IndexedRow(1, catalogVectorB),
      IndexedRow(2, catalogVectorC),
      IndexedRow(3, catalogVectorD)
    )

    val queryMatrix = new IndexedRowMatrix(sc.parallelize(queryRows))
    val catalogMatrix = new IndexedRowMatrix(sc.parallelize(catalogRows))

    val queryNearestNeighbour = new QueryNearestNeighbours(Cosine, 0.4, 1.0, 1.0)
    val expected = Seq(
      MatrixEntry(0, 0, Cosine(queryVectorA, catalogVectorA)),
      MatrixEntry(0, 3, Cosine(queryVectorA, catalogVectorD)),
      MatrixEntry(1, 1, Cosine(queryVectorB, catalogVectorB)),
      MatrixEntry(1, 2, Cosine(queryVectorB, catalogVectorC))
    )

    val got = queryNearestNeighbour.join(queryMatrix, catalogMatrix).entries.collect
    got should be(expected)
  }
}

