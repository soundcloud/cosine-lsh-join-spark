package com.soundcloud.lsh

import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{ Matrices, Matrix, Vector }
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.BitSet

import scala.util.Random

/**
 * Find the k nearest neighbors from a dataset for every other object in the
 * same dataset. Implementations may be either exact or approximate.
 *
 * @param matrix a row oriented matrix. Each row in the matrix represents
 *               a record in the dataset. Records are identified by their
 *               matrix index.
 * @return a similarity matrix with MatrixEntry(index_a, index_b, similarity).
 *
 */
trait Joiner {
  def join(matrix: IndexedRowMatrix): CoordinateMatrix
}

/**
 * Lsh implementation as described in 'Randomized Algorithms and NLP: Using
 * Locality Sensitive Hash Function for High Speed Noun Clustering' by
 * Ravichandran et al. See original publication for a detailed description of
 * the parameters.
 *
 * @param minCosineSimilarity minimum similarity two items need to have
 *                            otherwise they are discarded from the result set
 * @param dimensions number of random vectors (hyperplanes) to generate bit
 *                   vectors of length d
 * @param numNeighbours beam factor e.g. how many neighbours are considered
 *                      in the sliding window
 * @param numPermutations number of times bitsets are permuted
 *
 */
class Lsh(minCosineSimilarity: Double,
          dimensions: Int,
          numNeighbours: Int,
          numPermutations: Int,
          partitions: Int = 200,
          storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK)
  extends Joiner with Serializable {

  import com.soundcloud.lsh.Lsh._

  @transient val randomGenerator = new Random(1)

  def join(inputMatrix: IndexedRowMatrix): CoordinateMatrix = {
    val numFeatures = inputMatrix.rows.first.vector.size
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
    new CoordinateMatrix(mergedNeighbours.distinct())
  }

  /**
   * Returns a local k by d matrix with random gaussian entries mean=0.0 and
   * std=1.0 
   *
   * This is a k by d matrix as it is multiplied by the input matrix
   *
   */
  def localRandomMatrix(d: Int, numFeatures: Int): Matrix = {
    val values = Array.fill(numFeatures * d)(random)
    Matrices.dense(numFeatures, d, values)
  }

  /**
   * Draws a random number with mean 0 and standard deviation of 1
   *
   */
  def random(): Double = {
    randomGenerator.nextGaussian()
  }


    /**
     * Converts a given input matrix to a bit set representation using random hyperplanes
     *
     */
  def matrixToBitSet(inputMatrix: IndexedRowMatrix, localRandomMatrix: Matrix): RDD[Signature] = {
    inputMatrix.multiply(localRandomMatrix).rows.map {
      indexedRow: IndexedRow =>
        val bitSet = vectorToBitSet(indexedRow.vector)
        Signature(indexedRow.index, indexedRow.vector, bitSet)
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
   * Permutes a signatures by a given permutation
   *
   */
  def permuteBitSet(signatures: RDD[Signature], permutation: Iterable[Int], d: Int): RDD[Signature] = {
    signatures.map {
      signature: Signature =>
        val permutedBitSet = permuteBitSet(signature.bitSet, permutation, d)
        Signature(signature.index, signature.vector, permutedBitSet)
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
    signatures.sortBy(signature => signature)
  }

  /**
   * Creates a sliding window
   *
   */
  def createSlidingWindow(signatures: RDD[Signature], b: Int): RDD[Seq[Signature]] = {
    new SlidingRDD[Signature](signatures, b)
  }


  def findNeighbours(signatures: RDD[Seq[Signature]], minCosineSimilarity: Double): RDD[MatrixEntry] = {
    signatures.flatMap {
      signature: Iterable[Signature] =>
        neighbours(signature, minCosineSimilarity)
    }
  }

  /**
   * Generate all pairs and emit if cosine of pair > minCosineSimilarity
   *
   */
  def neighbours(signatures: Iterable[Signature], minCosineSimilarity: Double): Iterator[MatrixEntry] = {
    signatures.
      toSeq.
      sortBy(_.index). // sort in order to create an upper triangular matrix
      combinations(2).
      map {
      case first :: other :: nil =>
        val cosine = Cosine(first.vector, other.vector)
        MatrixEntry(first.index, other.index, cosine)
    }.
      filter(_.value >= minCosineSimilarity)
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
}

object Lsh {

  /**
   * Defines lexicographical ordering of bit set
   *
   */
  final case class Signature(index: Long, vector: Vector, bitSet: BitSet) extends Ordered[Signature] {
    def compare(that: Signature): Int = bitSetComparator(this.bitSet, that.bitSet)

    override def toString(): String = {
      val bs = bitSet
      var str = s"$index -> ["
      for (i <- bs.iterator) {
        str += s"$i,"
      }
      str + "]"
    }
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
}
