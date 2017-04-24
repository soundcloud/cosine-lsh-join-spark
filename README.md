# Cosine LSH Join Spark

A spark library for approximate nearest neighbours (ANN).

# Background

In many computational problems such as NLP, Recommendation Systems and Search,
items (e.g. words) are represented as vectors in a multidimensional space.
Then given a specific item it's nearest neighbours need to be find e.g. given
a query find the most similar ones. A naive liner scan over the data set might
be too slow for most data sets.

Hence, more efficient algorithms are needed. One of the most widely used
approaches is Locality Sensitive Hashing (LSH). This family of algorithms are
very fast but might not give the exact solution and are hence called
approximate nearest neighbours (ANN). The trade off between accuracy and speed
is generally set via parameters of the algorithm.

# Joiner Interface

This is an interface to find the k nearest neighbors from a data set for every other object in the
   same data set. Implementations may be either exact or approximate.

    trait Joiner {
        def join(matrix: IndexedRowMatrix): CoordinateMatrix
    }

matrix is a row oriented matrix. Each row in the matrix represents
an item in the dataset. Items are identified by their
matrix index.
Returns a similarity matrix with MatrixEntry(itemA, itemB, similarity).

# Example
    // item_a --> (1.0, 1.0, 1.0)
	// item_b --> (2.0, 2.0, 2.0)
	// item_c --> (6.0, 3.0, 2.0)

	val rows = Seq(
      IndexedRow(1, Vectors.dense(1.0, 1.0, 1.0)),
      IndexedRow(2, Vectors.dense(2.0, 2.0, 2.0)),
      IndexedRow(5, Vectors.dense(6.0, 3.0, 2.0))
    )
    val matrix = new IndexedRowMatrix(sc.parallelize(rows))
    val similariyMatrix = joiner.join(matrix)

    val results = similariyMatrix.entries.map {
          entry =>
            "item:%d item:%d cosine:%.2f".format(entry.i, entry.j, entry.value)
        }

    results.foreach(println)

    // above will print:
    // item:2 item:3 cosine:0,87
    // item:1 item:3 cosine:0,87
    // item:1 item:2 cosine:1,00

Please see included **Main.scala** file for a more detailed example.

## Implementations of the joiner interface

### LSH
This is an implementation of the following paper for Spark:

[Randomized Algorithms and NLP: Using Locality Sensitive Hash Function for High Speed Noun Clustering](http://dl.acm.org/citation.cfm?id=1219917)

-- <cite>Ravichandran et al.</cite>

The algorithm determines a set of candidate items in the first stage and only computes the exact cosine similarity for those candidates. It has been succesfully used in production with typical run times of a couple of minutes for millions of items.

Note that candidates are ranked by their exact cosine similarity. Hence, this algorithm will not return any false positives (items that the system thinks are nearby but are actually not). Most real world applications require this e.g. in recommendation systems it is ok to return similar items which are almost as good as the exact nearest neighbours but showing false positives would result in senseless recommendations.

    val lsh = new Lsh(
      minCosineSimilarity = 0.5,
      dimensions = 2,
      numNeighbours = 3,
      numPermutations = 1,
      partitions = 1,
      storageLevel = StorageLevel.MEMORY_ONLY
    )

Please see the original publication for a detailed description of the parameters.

### NearestNeighbours
Brute force method to compute exact nearest neighbours.
As this is a very expensive computation O(n^2) an additional sample parameter may be passed such
that neighbours are just computed for a random fraction.
This interface may be used to tune parameters for approximate solutions
on a small subset of data.

# QueryJoiner Interface
An interface to find the nearest neighbours in a catalog matrix for each entry in a query matrix.
Implementations may be either exact or approximate.

    trait QueryJoiner {
      def join(queryMatrix: IndexedRowMatrix, catalogMatrix: IndexedRowMatrix): CoordinateMatrix
    }

## Implementations of the QueryJoiner Interface

### QueryLsh
Standard Lsh implementation. A query matrix is hashed multiple times and exact hash matches are searched for in a catalog Matrix. These candidates are used to compute the exact cosine distance.

### QueryHamming

Implementation based on approximated cosine distances. The cosine distances are
approximated using hamming distances which are way faster to compute.
The catalog matrix is broadcasted. This implementation is therefore suited for
tasks where the catalog matrix is very small compared to the query matrix.

### QueryNearestNeighbours
Brute force O(size(query) * size(catalog)) method to compute exact nearest neighbours for rows in the query matrix. As this is a very expensive computation additional sample parameters may be passed such that neighbours are just computed for a random fraction of the data set. This interface may be used to tune parameters for approximate solutions on a small subset of data.

# Maven
The artifacts are hosted on Maven Central. For Spark 1.x add the following line to your build.sbt file:

	libraryDependencies += "com.soundcloud" % "cosine-lsh-join-spark_2.10" % "0.0.5"

For Spark 2.x use:

    libraryDependencies += "com.soundcloud" % "cosine-lsh-join-spark_2.10" % "1.0.1"

or if you're on scala 2.11.x use:

    libraryDependencies += "com.soundcloud" % "cosine-lsh-join-spark_2.11" % "1.0.1"


#Contributors

[Özgür Demir](https://github.com/ozgurdemir)

[Rany Keddo](https://github.com/purzelrakete/)

[Alexey Rodriguez Yakushev](https://github.com/alexeyrodriguez)

[Aaron Levin](https://github.com/aaronlevin)
