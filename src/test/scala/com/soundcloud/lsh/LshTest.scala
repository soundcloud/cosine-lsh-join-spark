package com.soundcloud.lsh

import com.soundcloud.TestHelper
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.scalatest.{FunSuite, Matchers}

class LshTest extends FunSuite with SparkLocalContext with Matchers with TestHelper {

  val lsh = new Lsh(
    minCosineSimilarity = -1.0,
    dimensions = 1000,
    numNeighbours = 20,
    numPermutations = 2)

  test("lsh") {
    val vector0 = Vectors.dense(1, 1, 0, 0)
    val vector1 = Vectors.dense(1, 2, 0, 0)
    val vector2 = Vectors.dense(0, 1, 4, 2)

    val rows = Seq(
      IndexedRow(0, vector0),
      IndexedRow(1, vector1),
      IndexedRow(2, vector2)
    )
    val inputMatrix = new IndexedRowMatrix(sc.parallelize(rows))
    val got = lsh.join(inputMatrix).entries.collect
    val expected = Seq(
      (0, 2, Cosine(vector0, vector2)),
      (1, 2, Cosine(vector1, vector2)),
      (0, 1, Cosine(vector0, vector1))
    )
    implicit val equality = new MatrixEquality(0.01)
    got.sortBy(_.value) should equal (expected)
  }

  test("permute signature") {
    val bitSet = new BitSet(3)
    bitSet.set(0)
    bitSet.set(1)
    val permutation = Array(2, 1, 0)
    val got = lsh.permuteBitSet(bitSet, permutation, 3)
    val expected = new BitSet(3)
    expected.set(1)
    expected.set(2)
    bitSetIsEqual(got, expected) should be(true)
  }

  test("generate permutation") {
    val got = lsh.generatePermutation(10)
    got should have size 10
    val expected = (0 until 10).toArray
    got.toSeq.sorted should be(expected)
  }

  test("order by bitset") {
    val bitSet1 = new BitSet(4)
    bitSet1.set(0)
    val bitSet2 = new BitSet(4)
    val bitSet3 = new BitSet(4)
    bitSet3.set(1)
    val vector = Vectors.dense(1, 2, 3)
    val signatures = Seq(
      Signature(0, vector, bitSet1),
      Signature(1, vector, bitSet2),
      Signature(2, vector, bitSet3)
    )
    val got = lsh.orderByBitSet(sc.parallelize(signatures)).collect().toSeq
    got(0).index should be(1)
    got(1).index should be(2)
    got(2).index should be(0)
  }

  def gotIndexOnly(input: Array[Array[Signature]]): Array[Array[Long]] = {
    input.map {
      window: Array[Signature] =>
        window.map(_.index)
    }
  }

  val vector = Vectors.dense(1, 2, 3)
  val signatures = Seq(
    Signature(3, vector, new BitSet(4)),
    Signature(4, vector, new BitSet(4)),
    Signature(1, vector, new BitSet(4)),
    Signature(2, vector, new BitSet(4)),
    Signature(5, vector, new BitSet(4))
  )

  test("sliding window") {
    val got = lsh.createSlidingWindow(sc.parallelize(signatures, 10), 3)
    val expected = Seq(
      List(3, 4, 1),
      List(2, 5)
    )
    gotIndexOnly(got.collect) should be(expected)
  }

  test("sliding window partial") {
    val got2 = lsh.createSlidingWindow(sc.parallelize(signatures, 10), 10)
    val expected2 = Seq(
      List(3, 4, 1, 2, 5)
    )
    gotIndexOnly(got2.collect) should be(expected2)
  }

  test("neighbours") {
    val signatures = Array(
      Signature(3, Vectors.dense(0, 1), new BitSet(4)),
      Signature(4, Vectors.dense(0, 1), new BitSet(4)),
      Signature(1, Vectors.dense(0, 1), new BitSet(4)),
      Signature(2, Vectors.dense(1, 0), new BitSet(4)) // filtered as cosine is 0.0 with all other
    )

    val got = lsh.neighbours(signatures, 1.0)
    val expected = Seq(
      new MatrixEntry(1, 3, 1.0),
      new MatrixEntry(1, 4, 1.0),
      new MatrixEntry(3, 4, 1.0)
    )
    got.toSeq should be(expected)
  }

}
