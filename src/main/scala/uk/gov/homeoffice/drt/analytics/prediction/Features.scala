package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import uk.gov.homeoffice.drt.analytics.prediction.FeatureType.{FeatureType, OneToMany, Single}

import scala.collection.immutable

case class Features(featureTypes: List[FeatureType], oneToManyValues: IndexedSeq[String]) {
  def oneToManyIndices(row: Row): immutable.Seq[Int] =
    oneToManyFeatures
      .map { fs =>
        val featureColValues = fs.columnNames.map(c => row.getAs[String](c))
        val featureString = s"${fs.featurePrefix}_${featureColValues.mkString("-")}"
        oneToManyValues.indexOf(featureString)
      }
      .filter(_ >= 0)

  def oneToManyFeatures: immutable.Seq[OneToMany] =
    featureTypes.collect {
      case otm: OneToMany => otm
    }

  def singleFeatures: immutable.Seq[Single] =
    featureTypes.collect {
      case otm: Single => otm
    }

  def oneToManyFeaturesIndices(row: Row): Array[Double] =
    Vectors.sparse(oneToManyValues.size, oneToManyIndices(row).map(idx => (idx, 1d))).toArray

  def singleFeaturesVector(row: Row): List[Double] = featureTypes.collect {
    case Single(columnName) => row.getAs[Double](columnName)
  }

  def featuresVectorForRow(row: Row): linalg.Vector =
    Vectors.dense(oneToManyFeaturesIndices(row) ++ singleFeaturesVector(row))

  def labelAndFeatureCols(allColumns: Iterable[String], labelColName: String)
                         (implicit session: SparkSession): immutable.Seq[Column] = {
    val featureColumns = featureTypes.map {
      case OneToMany(columnNames, _) => concat_ws("-", columnNames.map(col): _*)
      case Single(columnName) => col(columnName)
    }
    col(labelColName) :: (featureColumns ++ allColumns.map(col))
  }
}

object Features {
  def apply(df: DataFrame, featureSpecs: List[FeatureType]): Features = {
    val oneToManyValues = featureSpecs.flatMap {
      case _: Single => Iterable()
      case OneToMany(columnNames, featurePrefix) =>
        df
          .select(concat_ws("-", columnNames.map(col): _*))
          .rdd.distinct.collect
          .map(r => s"${featurePrefix}_${r.getAs[String](0)}")
    }.toIndexedSeq
    Features(featureSpecs, oneToManyValues)
  }
}
