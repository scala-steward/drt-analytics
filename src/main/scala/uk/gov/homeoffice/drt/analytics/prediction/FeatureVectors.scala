package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Row}
import uk.gov.homeoffice.drt.prediction.FeaturesWithOneToManyValues
import uk.gov.homeoffice.drt.prediction.arrival.FeatureColumns.Single

import scala.collection.immutable
import scala.util.{Failure, Success, Try}

object FeatureVectors {
  def featuresVectorForRow(row: Row, features: FeaturesWithOneToManyValues): linalg.Vector =
    Vectors.dense(oneToManyFeaturesIndices(row, features) ++ singleFeaturesVector(row, features))

  def oneToManyFeaturesIndices(row: Row, features: FeaturesWithOneToManyValues): Array[Double] = {
    val sortedIndices = oneToManyIndices(row, features).sorted.map(idx => (idx, 1d))

    Try(Vectors.sparse(features.oneToManyValues.size, sortedIndices).toArray) match {
      case Success(arr) => arr
      case Failure(t) =>
        throw new Exception(s"Failed to create sparse vector from ${features.oneToManyValues.size} indices: $sortedIndices", t)
    }
  }

  def singleFeaturesVector(row: Row, features: FeaturesWithOneToManyValues): List[Double] = features.features.collect {
    case feature: Single[_] => row.getAs[Double](feature.label)
  }

  def oneToManyIndices(row: Row, features: FeaturesWithOneToManyValues): Seq[Int] =
    features.oneToManyFeatures
      .map { fs =>
        val featureValue = row.getAs[String](fs.label)
        features.oneToManyValues.indexOf(featureValue)
      }
      .filter(_ >= 0)

  def labelAndFeatureCols(allColumns: Iterable[String], labelColName: String): immutable.Seq[Column] =
    col(labelColName) +: allColumns.map(col).toList
}
