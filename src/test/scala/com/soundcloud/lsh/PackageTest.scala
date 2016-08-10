package com.soundcloud.lsh

import java.util.Random

import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.mllib.linalg.distributed.{IndexedRowMatrix, IndexedRow}
import org.apache.spark.util.collection.BitSet
import org.scalatest.{Matchers, FunSuite}

class PackageTest extends FunSuite with SparkLocalContext with Matchers {

  test("local lsh.random matrix") {
    val numRows = 5
    val numColumns = 10
    val got = localRandomMatrix(numRows, numColumns)
    got.numRows should be(numColumns) // transposed matrix
    got.numCols should be(numRows) // transposed matrix
  }

  test("matrix to bitset") {
    val rows = Seq(
      IndexedRow(0, Vectors.dense(1.0, 0.0, 0.0)),
      IndexedRow(10, Vectors.dense(0.0, 1.0, 0.0))
    )
    val inputMatrix = new IndexedRowMatrix(sc.parallelize(rows))

    val randomMatrix = Matrices.dense(3, 2, Array(1.0, -1.0, 1.0, -1.0, 1.0, -1.0))
    val got = matrixToBitSet(inputMatrix, randomMatrix).collect()
    val expectedBitVec1 = new BitSet(2)
    expectedBitVec1.set(0)
    got(0).index should be(0)
    bitSetIsEqual(got(0).bitSet, expectedBitVec1) should be(true)

    val expectedBitVec2 = new BitSet(2)
    expectedBitVec2.set(1)
    got(1).index should be(10)
    bitSetIsEqual(got(1).bitSet, expectedBitVec2) should be(true)

    got.size should be (2)
  }

  test("vector to bitset") {
    val values = Array(-1.0, 1.0, 5.0)
    val vector = Vectors.dense(values)
    val got = vectorToBitSet(vector)
    val expected = new BitSet(3)
    expected.set(1)
    expected.set(2)
    bitSetIsEqual(got, expected) should be(true)
  }

  test("hamming approximation") {
    val randomGenerator = new Random()
    val d = 1000 // need this as otherwise results are bad
    val k = 150
    val rows = Seq(
      IndexedRow(0, Vectors.dense(Array.fill(k)(randomGenerator.nextGaussian()))),
      IndexedRow(1, Vectors.dense(Array.fill(k)(randomGenerator.nextGaussian()))),
      IndexedRow(2, Vectors.dense(Array.fill(k)(randomGenerator.nextGaussian())))
    )
    val cosine01 = Cosine(rows(0).vector, rows(1).vector)
    val cosine02 = Cosine(rows(0).vector, rows(2).vector)
    val cosine12 = Cosine(rows(1).vector, rows(2).vector)

    val inputMatrix = new IndexedRowMatrix(sc.parallelize(rows))
    val randomMatrix = localRandomMatrix(d, k)
    val signatures = matrixToBitSet(inputMatrix, randomMatrix).collect().toSeq
    signatures(0).index should be(0)
    signatures(1).index should be(1)

    val hamming01 = hamming(signatures(0).bitSet, signatures(1).bitSet)
    val hamming02 = hamming(signatures(0).bitSet, signatures(2).bitSet)
    val hamming12 = hamming(signatures(1).bitSet, signatures(2).bitSet)

    val approxCosine01 = hammingToCosine(hamming01, d)
    val approxCosine02 = hammingToCosine(hamming02, d)
    val approxCosine12 = hammingToCosine(hamming12, d)

    val tollerance = 0.1
    approxCosine01 should be((cosine01) +- tollerance)
    approxCosine02 should be((cosine02) +- tollerance)
    approxCosine12 should be((cosine12) +- tollerance)
  }

  test("bit set comparator") {
    val vec1 = new BitSet(4)
    val vec2 = new BitSet(4)
    bitSetComparator(vec1, vec2) should be(0)
    vec1.set(0)
    bitSetComparator(vec1, vec2) should be(+1)

    val vec3 = new BitSet(4)
    vec3.set(1)
    bitSetComparator(vec3, vec2) should be(+1)

    bitSetComparator(vec1, vec3) should be(+1)
  }

  test("hamming to cosine") {
    val vec1 = new BitSet(4)
    val vec2 = new BitSet(4)
    vec1.set(0)
    vec2.set(2)
    val hammingDistance = hamming(vec1, vec2)
    val got = hammingToCosine(hammingDistance, 4)
    val pr = 1.0 - 2.0 / 4.0
    val expected = math.cos((1.0 - pr) * math.Pi)
    got should be(expected)
  }

  test("hamming") {
    val vec1 = new BitSet(4)
    val vec2 = new BitSet(4)
    vec1.set(0)
    vec2.set(2)
    val got = hamming(vec1, vec2)
    got should be(2)
  }

  test("bitset is equal") {
    val vec1 = new BitSet(4)
    vec1.set(1)

    val vec2 = new BitSet(4)
    bitSetIsEqual(vec1, vec2) should be(false)

    vec2.set(1)
    bitSetIsEqual(vec1, vec2) should be(true)
  }

  test("bitset to string") {
    val bs = new BitSet(4)
    bitSetToString(bs) should be("")
    bs.set(1)
    bitSetToString(bs) should be("1")
    bs.set(3)
    bitSetToString(bs) should be("1,3")
  }

}
