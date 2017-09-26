package com.soundcloud.lsh

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD


/**
 * Implementation based on approximated cosine distances. The cosine distances are
 * approximated using hamming distances which are way faster to compute.
 * Either the catalog matrix or the query matrix is broadcasted.
 * This implementation is therefore suited for tasks where one of the matrices
 * is very small (in order to be broadcastet) compared to the query matrix.
 *
 * @param minCosineSimilarity minimum similarity two items need to have
 *                            otherwise they are discarded from the result set
 * @param dimensions          number of random vectors (hyperplanes) to generate bit
 *                            vectors of length d. Should be considerably large in order
 *                            to get good approximations of the cosine distances e.g. 500
 *                            if input size = 150
 * @param resultSize          number of results for each entry in query matrix
 * @param broadcastCatalog    if true the catalog matrix is broadcasted otherwise the
 *                            query matrix
 *
 */
class QueryHamming(minCosineSimilarity: Double,
                   dimensions: Int,
                   resultSize: Int,
                   broadcastCatalog: Boolean = true) extends QueryJoiner with Serializable {

  override def join(queryMatrix: IndexedRowMatrix, catalogMatrix: IndexedRowMatrix): CoordinateMatrix = {
    val numFeatures = queryMatrix.numCols().toInt

    val randomMatrix = localRandomMatrix(dimensions, numFeatures)
    val querySignatures = matrixToBitSetSparse(queryMatrix, randomMatrix)
    val catalogSignatures = matrixToBitSetSparse(catalogMatrix, randomMatrix)

    var rddSignatures: RDD[SparseSignature] = null
    var broadcastSignatures: Broadcast[Array[SparseSignature]] = null

    if (broadcastCatalog) {
      rddSignatures = querySignatures
      broadcastSignatures = querySignatures.sparkContext.broadcast(catalogSignatures.collect)
    } else {
      rddSignatures = catalogSignatures
      broadcastSignatures = catalogSignatures.sparkContext.broadcast(querySignatures.collect)
    }

    val approximated = rddSignatures.mapPartitions {
      rddSignatureIterator =>
        val signaturesBC = broadcastSignatures.value
        rddSignatureIterator.flatMap {
          rddSignature =>
            signaturesBC.map {
              broadCastSignature =>
                val approximatedCosine = hammingToCosine(hamming(rddSignature.bitSet, broadCastSignature.bitSet), dimensions)

                if (broadcastCatalog)
                  new MatrixEntry(rddSignature.index, broadCastSignature.index, approximatedCosine)
                else
                  new MatrixEntry(broadCastSignature.index, rddSignature.index, approximatedCosine)
            }.filter(_.value >= minCosineSimilarity).sortBy(-_.value).take(resultSize)
        }
    }
    broadcastSignatures.unpersist(true)

    new CoordinateMatrix(approximated)
  }

}
