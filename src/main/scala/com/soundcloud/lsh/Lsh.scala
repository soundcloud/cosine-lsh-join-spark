package com.soundcloud.lsh

import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.BitSet

/**
 * Lsh implementation as described in 'Randomized Algorithms and NLP: Using
 * Locality Sensitive Hash Function for High Speed Noun Clustering' by
 * Ravichandran et al. See original publication for a detailed description of
 * the parameters.
 *
 * @see http://dl.acm.org/citation.cfm?id=1219917
 * @param minCosineSimilarity minimum similarity two items need to have
 *                            otherwise they are discarded from the result set
 * @param dimensions          number of random vectors (hyperplanes) to generate bit
 *                            vectors of length d
 * @param numNeighbours       beam factor e.g. how many neighbours are considered
 *                            in the sliding window
 * @param numPermutations     number of times bitsets are permuted
 *
 */
class Lsh(minCosineSimilarity: Double,
          dimensions: Int,
          numNeighbours: Int,
          numPermutations: Int,
          partitions: Int = 200,
          storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK)
  extends Joiner with Serializable {

  override def join(inputMatrix: IndexedRowMatrix): CoordinateMatrix = {
    val numFeatures = inputMatrix.numCols().toInt
    val randomMatrix = localRandomMatrix(dimensions, numFeatures)
    val signatures = matrixToBitSet(inputMatrix, randomMatrix).
      repartition(partitions).
      persist(storageLevel)

    val neighbours = 0 until numPermutations map {
      x =>
        val permutation = generatePermutation(dimensions)
        val permuted = permuteBitSet(signatures, permutation, dimensions)
        val orderedSignatures = orderByBitSet(permuted)
        val slidingWindow = createSlidingWindow(orderedSignatures, numNeighbours)
        findNeighbours(slidingWindow, minCosineSimilarity)
    }

    val mergedNeighbours = neighbours.reduce(_ ++ _)
    new CoordinateMatrix(distinct(mergedNeighbours))
  }

  /**
   * Permutes a signatures by a given permutation
   *
   */
  def permuteBitSet(signatures: RDD[Signature], permutation: Iterable[Int], d: Int): RDD[Signature] = {
    signatures.map {
      signature: Signature =>
        val permutedBitSet = permuteBitSet(signature.bitSet, permutation, d)
        signature.copy(bitSet = permutedBitSet)
    }
  }

  /**
   * Permutes a bit set representation of a vector by a given permutation
   */
  def permuteBitSet(bitSet: BitSet, permutation: Iterable[Int], d: Int): BitSet = {
    val permutationWithIndex = permutation.zipWithIndex
    val newBitSet = new BitSet(d)
    permutationWithIndex.foreach {
      case ((newIndex: Int, oldIndex: Int)) =>
        val oldBit = bitSet.get(oldIndex)
        if (oldBit)
          newBitSet.set(newIndex)
        else
          newBitSet.unset(newIndex)
    }
    newBitSet
  }

  /**
   * Generates a random permutation of size n
   */
  def generatePermutation(size: Int): Iterable[Int] = {
    val indices = (0 until size).toArray
    util.Random.shuffle(indices)
  }

  /**
   * Orderes an RDD of signatures by their bit set representation
   */
  def orderByBitSet(signatures: RDD[Signature]): RDD[Signature] = {
    signatures.sortBy(identity)
  }

  /**
   * Creates a sliding window
   *
   */
  def createSlidingWindow(signatures: RDD[Signature], b: Int): RDD[Array[Signature]] = {
    new SlidingRDD[Signature](signatures, b, b)
  }


  def findNeighbours(signatures: RDD[Array[Signature]], minCosineSimilarity: Double): RDD[MatrixEntry] = {
    signatures.flatMap { signature: Array[Signature] =>
        neighbours(signature, minCosineSimilarity)
    }
  }

  /**
   * Generate all pairs and emit if cosine of pair > minCosineSimilarity
   *
   */
  def neighbours(signatures: Array[Signature], minCosineSimilarity: Double): Iterator[MatrixEntry] = {
    signatures.
      sortBy(_.index). // sort in order to create an upper triangular matrix
      combinations(2).
      map {
        case Array(first, second) =>
          val cosine = Cosine(first.vector, second.vector)
          MatrixEntry(first.index, second.index, cosine)
      }.
      filter(_.value >= minCosineSimilarity)
  }

}
