package com.soundcloud.lsh

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRowMatrix, MatrixEntry}


/**
 * Implementation based on approximated cosine distances. The cosine distances are
 * approximated using hamming distances which are way faster to compute.
 * The catalog matrix is broadcasted. This implementation is therefore suited for
 * tasks where the catalog matrix is very small compared to the query matrix.
 *
 * @param minCosineSimilarity minimum similarity two items need to have
 *                            otherwise they are discarded from the result set
 * @param dimensions          number of random vectors (hyperplanes) to generate bit
 *                            vectors of length d. Should be considerably large in order
 *                            to get good approximations of the cosine distances e.g. 500
 *                            if input size = 150
 * @param resultSize          number of results for each entry in query matrix
 *
 */
class QueryHamming(minCosineSimilarity: Double,
                   dimensions: Int,
                   resultSize: Int) extends QueryJoiner with Serializable {

  override def join(queryMatrix: IndexedRowMatrix, catalogMatrix: IndexedRowMatrix): CoordinateMatrix = {
    val numFeatures = queryMatrix.numCols().toInt

    val randomMatrix = localRandomMatrix(dimensions, numFeatures)
    val querySignatures = matrixToBitSetSparse(queryMatrix, randomMatrix)
    val catalogSignatures = matrixToBitSetSparse(catalogMatrix, randomMatrix)

    val catalogPool = querySignatures.sparkContext.broadcast(catalogSignatures.collect)

    val approximated = querySignatures.mapPartitions {
      queries =>
        val catalog = catalogPool.value
        queries.flatMap {
          query =>
            catalog.map {
              catalog =>
                val approximatedCosine = hammingToCosine(hamming(query.bitSet, catalog.bitSet), dimensions)
                new MatrixEntry(query.index, catalog.index, approximatedCosine)
            }.filter(_.value >= minCosineSimilarity).sortBy(-_.value).take(resultSize)
        }
    }
    catalogPool.unpersist(true)

    new CoordinateMatrix(approximated)
  }

}
