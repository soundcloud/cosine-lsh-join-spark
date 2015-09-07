package com.soundcloud.lsh

import com.soundcloud.lsh.Lsh.Signature
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.util.collection.BitSet
import org.scalatest.{FunSuite, Matchers}

class LshTest
  extends FunSuite
  with SparkLocalContext
  with Matchers {

  val lsh = new Lsh(
    minCosineSimilarity = -1.0,
    dimensions = 1000,
    numNeighbours = 20,
    numPermutations = 2)

  test("lsh") {
    val rows = Seq(
      IndexedRow(0, Vectors.dense(1, 1, 0, 0)),
      IndexedRow(1, Vectors.dense(1, 2, 0, 0)),
      IndexedRow(2, Vectors.dense(0, 1, 4, 2))
    )
    val inputMatrix = new IndexedRowMatrix(sc.parallelize(rows))
    val got = lsh.join(inputMatrix)
    val expected = Seq(
      (0, 1),
      (0, 2),
      (1, 2)
    )
    val gotIndex = got.entries.collect().map {
      entry: MatrixEntry =>
        (entry.i, entry.j)
    }
    gotIndex.sorted should be(expected.sorted)
  }

  test("bitset encoding") {
    val d = 1000 // need this as otherwise results are bad
    val k = 150
    val rows = Seq(
      IndexedRow(0, Vectors.dense(Array.fill(k)(lsh.random))),
      IndexedRow(1, Vectors.dense(Array.fill(k)(lsh.random))),
      IndexedRow(2, Vectors.dense(Array.fill(k)(lsh.random)))
    )
    val cosine01 = Cosine(rows(0).vector, rows(1).vector)
    val cosine02 = Cosine(rows(0).vector, rows(2).vector)
    val cosine12 = Cosine(rows(1).vector, rows(2).vector)

    val inputMatrix = new IndexedRowMatrix(sc.parallelize(rows))
    val randomMatrix = lsh.localRandomMatrix(d, k)
    val signatures = lsh.matrixToBitSet(inputMatrix, randomMatrix).collect().toSeq
    signatures(0).index should be(0)
    signatures(1).index should be(1)

    val hamming01 = lsh.hamming(signatures(0).bitSet, signatures(1).bitSet)
    val hamming02 = lsh.hamming(signatures(0).bitSet, signatures(2).bitSet)
    val hamming12 = lsh.hamming(signatures(1).bitSet, signatures(2).bitSet)

    val approxCosine01 = lsh.hammingToCosine(hamming01, d)
    val approxCosine02 = lsh.hammingToCosine(hamming02, d)
    val approxCosine12 = lsh.hammingToCosine(hamming12, d)

    val tollerance = 0.1
    approxCosine01 should be((cosine01) +- tollerance)
    approxCosine02 should be((cosine02) +- tollerance)
    approxCosine12 should be((cosine12) +- tollerance)

  }

  test("local lsh.random matrix") {
    val numRows = 5
    val numColumns = 10
    val got = lsh.localRandomMatrix(numRows, numColumns)
    got.numRows should be(numColumns) // transposed matrix
    got.numCols should be(numRows) // transposed matrix
  }

  test("matrix to bitset") {
    val rows = Seq(
      IndexedRow(0, Vectors.dense(1.0, 0.0, 0.0)),
      IndexedRow(1, Vectors.dense(0.0, 1.0, 0.0))
    )
    val inputMatrix = new IndexedRowMatrix(sc.parallelize(rows))

    val randomMatrix = Matrices.dense(3, 2, Array(1.0, -1.0, 1.0, -1.0, 1.0, -1.0))
    val got = lsh.matrixToBitSet(inputMatrix, randomMatrix).collect()
    val expectedBitVec1 = new BitSet(2)
    expectedBitVec1.set(0)
    got(0).index should be(0)
    lsh.bitSetIsEqual(got(0).bitSet, expectedBitVec1) should be(true)

    val expectedBitVec2 = new BitSet(2)
    expectedBitVec2.set(1)
    got(1).index should be(1)
    lsh.bitSetIsEqual(got(1).bitSet, expectedBitVec2) should be(true)
  }

  test("vector to bitset") {
    val values = Array(-1.0, 1.0, 5.0)
    val vector = Vectors.dense(values)
    val got = lsh.vectorToBitSet(vector)
    val expected = new BitSet(3)
    expected.set(1)
    expected.set(2)
    lsh.bitSetIsEqual(got, expected) should be(true)
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
    lsh.bitSetIsEqual(got, expected) should be(true)
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
    val vector = Vectors.dense(1,2,3)
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

  test("bit set comparator") {
    val vec1 = new BitSet(4)
    val vec2 = new BitSet(4)
    Lsh.bitSetComparator(vec1, vec2) should be(0)
    vec1.set(0)
    Lsh.bitSetComparator(vec1, vec2) should be(+1)

    val vec3 = new BitSet(4)
    vec3.set(1)
    Lsh.bitSetComparator(vec3, vec2) should be(+1)

    Lsh.bitSetComparator(vec1, vec3) should be(+1)
  }

  test("sliding window") {
    val vector = Vectors.dense(1,2,3)
    val signatures = Seq(
      Signature(3, vector, new BitSet(4)),
      Signature(4, vector, new BitSet(4)),
      Signature(1, vector, new BitSet(4)),
      Signature(2, vector, new BitSet(4)),
      Signature(5, vector, new BitSet(4))
    )
    val got = lsh.createSlidingWindow(sc.parallelize(signatures), 3)
    val expected = Seq(
      List(3, 4, 1),
      List(2, 5)
    )
    // fot easier comparison
    val gotWithoutBitSet = got.collect.toSeq.map {
      window: Iterable[Signature] =>
        window.map(_.index)
    }

    gotWithoutBitSet should be(expected)
  }

  test("hamming to cosine") {
    val vec1 = new BitSet(4)
    val vec2 = new BitSet(4)
    vec1.set(0)
    vec2.set(2)
    val hammingDistance = lsh.hamming(vec1, vec2)
    val got = lsh.hammingToCosine(hammingDistance, 4)
    val pr = 1.0 - 2.0 / 4.0
    val expected = math.cos((1.0 - pr) * math.Pi)
    got should be(expected)
  }

  test("neighbours") {
    val signatures = Seq(
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

  test("hamming") {
    val vec1 = new BitSet(4)
    val vec2 = new BitSet(4)
    vec1.set(0)
    vec2.set(2)
    val got = lsh.hamming(vec1, vec2)
    got should be(2)
  }

  test("bitset is equal") {
    val vec1 = new BitSet(4)
    vec1.set(1)

    val vec2 = new BitSet(4)
    lsh.bitSetIsEqual(vec1, vec2) should be(false)

    vec2.set(1)
    lsh.bitSetIsEqual(vec1, vec2) should be(true)
  }
}
