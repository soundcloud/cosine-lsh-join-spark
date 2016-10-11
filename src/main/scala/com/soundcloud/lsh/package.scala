package com.soundcloud

import java.util.Random

import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.BitSet

package object lsh {

  /**
   * An id with it's hash encoding.
   *
   */
  final case class SparseSignature(index: Long, bitSet: BitSet) extends Ordered[SparseSignature] {
    override def compare(that: SparseSignature): Int = bitSetComparator(this.bitSet, that.bitSet)
  }

  /**
   * An id with it's hash encoding and original vector.
   *
   */
  final case class Signature(index: Long, vector: Vector, bitSet: BitSet) extends Ordered[Signature] {
    override def compare(that: Signature): Int = bitSetComparator(this.bitSet, that.bitSet)
  }

  /**
   * Compares two bit sets according to the first different bit
   *
   */
  def bitSetComparator(a: BitSet, b: BitSet): Int = {
    val xor = a ^ b
    val firstDifference = xor.nextSetBit(0)
    if (firstDifference >= 0) {
      if (a.get(firstDifference)) // if the difference is set to 1 on a
        1
      else
        -1
    } else {
      0
    }
  }

  /**
   * Returns a string representation of a BitSet
   */
  def bitSetToString(bs: BitSet): String = bs.iterator.mkString(",")


  /**
   * Returns a local k by d matrix with random gaussian entries mean=0.0 and
   * std=1.0
   *
   * This is a k by d matrix as it is multiplied by the input matrix
   *
   */
  def localRandomMatrix(d: Int, numFeatures: Int): Matrix = {
    val randomGenerator = new Random()
    val values = Array.fill(numFeatures * d)(randomGenerator.nextGaussian())
    Matrices.dense(numFeatures, d, values)
  }

  /**
   * Converts a given input matrix to a bit set representation using random hyperplanes
   *
   */
  def matrixToBitSet(inputMatrix: IndexedRowMatrix, localRandomMatrix: Matrix): RDD[Signature] = {
    val bitSets = inputMatrix.multiply(localRandomMatrix).rows.map {
      indexedRow =>
        (indexedRow.index, vectorToBitSet(indexedRow.vector))
    }
    val originalVectors = inputMatrix.rows.map { row => (row.index, row.vector) }
    bitSets.join(originalVectors).map {
      case (id, (bitSet, vector)) =>
        Signature(id, vector, bitSet)
    }
  }

  /**
   * Converts a given input matrix to a bit set representation using random hyperplanes
   *
   */
  def matrixToBitSetSparse(inputMatrix: IndexedRowMatrix, localRandomMatrix: Matrix): RDD[SparseSignature] = {
    inputMatrix.multiply(localRandomMatrix).rows.map {
      indexedRow: IndexedRow =>
        val bitSet = vectorToBitSet(indexedRow.vector)
        SparseSignature(indexedRow.index, bitSet)
    }
  }

  /**
   * Converts a vector to a bit set by replacing all values of x with sign(x)
   *
   */
  def vectorToBitSet(vector: Vector): BitSet = {
    val bitSet = new BitSet(vector.size)
    vector.toArray.zipWithIndex.map {
      case ((value: Double, index: Int)) =>
        if (math.signum(value) > 0)
          bitSet.set(index)
    }
    bitSet
  }

  /**
   * Approximates the cosine distance of two bit sets using their hamming
   * distance
   *
   */
  def hammingToCosine(hammingDistance: Int, d: Double): Double = {
    val pr = 1.0 - (hammingDistance / d)
    math.cos((1.0 - pr) * math.Pi)
  }

  /**
   * Returns the hamming distance between two bit vectors
   *
   */
  def hamming(vec1: BitSet, vec2: BitSet): Int = {
    (vec1 ^ vec2).cardinality()
  }

  /**
   * Compares two bit sets for their equality
   *
   */
  def bitSetIsEqual(vec1: BitSet, vec2: BitSet): Boolean = {
    hamming(vec1, vec2) == 0
  }

  /**
   * Take distinct matrix entry values based on the indices only.
   * The actual values are discarded.
   *
   */
  def distinct(matrix: RDD[MatrixEntry]): RDD[MatrixEntry] = {
    matrix.keyBy(m => (m.i, m.j)).reduceByKey((x, y) => x).values
  }

}
