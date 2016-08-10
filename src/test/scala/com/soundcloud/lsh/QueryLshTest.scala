package com.soundcloud.lsh

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.util.collection.BitSet
import org.scalatest.{FunSuite, Matchers}

class QueryLshTest extends FunSuite with SparkLocalContext with Matchers {

  val lsh = new QueryLsh(
    minCosineSimilarity = -1.0,
    dimensions = 100,
    rounds = 10)

  test("join bitmap") {
    val bitseq = Seq(
      new BitSet(2),
      new BitSet(2)
    )
    bitseq(0).set(0)
    bitseq(1).set(1)
    val rdd = sc.parallelize(bitseq.map(bitSetToString(_)).zipWithIndex)
    val joined = rdd.join(rdd).values.collect
    joined shouldBe Seq((0,0), (1,1))
  }

  test("join") {
    val rows = Seq(
      IndexedRow(0, Vectors.dense(1, 1, 0, 0)),
      IndexedRow(1, Vectors.dense(1, 2, 0, 0)),
      IndexedRow(2, Vectors.dense(0, 1, 4, 2))
    )
    val inputMatrix = new IndexedRowMatrix(sc.parallelize(rows))
    val got = lsh.join(inputMatrix, inputMatrix)
    val expected = Seq(
      (0, 0),
      (1, 1),
      (2, 2)
    )
    val gotIndex = got.entries.collect.map {
      entry: MatrixEntry =>
        (entry.i, entry.j)
    }
    gotIndex.sorted should be(expected.sorted)
  }

  test("computeCosine") {
    val query = Signature(3, Vectors.dense(0, 1), new BitSet(4))
    val candidate = Signature(4, Vectors.dense(0, 1), new BitSet(4))

    val got = lsh.computeCosine((query, candidate))
    val expected = new MatrixEntry(3, 4, 1.0)
    got should be(expected)
  }

  test("distinct") {
    val matrix = Seq(
      new MatrixEntry(1, 2, 3.4),
      new MatrixEntry(1, 2, 3.5),
      new MatrixEntry(1, 3, 3.4)
    )
    val got = distinct(sc.parallelize(matrix)).collect
    val expected = Seq(
      matrix(0), matrix(2)
    )
    got should be(expected)
  }

}
