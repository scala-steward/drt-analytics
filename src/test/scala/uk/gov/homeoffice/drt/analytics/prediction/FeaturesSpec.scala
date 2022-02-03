package uk.gov.homeoffice.drt.analytics.prediction

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import uk.gov.homeoffice.drt.prediction.FeatureType.OneToMany
import uk.gov.homeoffice.drt.prediction.Features

class FeaturesSpec extends AnyWordSpec with Matchers {
  val context: (SparkSession => Any) => Unit = (test: SparkSession => Any) => {
    implicit val session: SparkSession = newSparkSession

    test(session)

    session.close()
  }

  "Given a simple one column feature and a data frame, Features" should {
    "give an indexed seq containing the unique 2 values strings" in context {
      session =>
        import session.implicits._
        val featureTypes = List(OneToMany(List("a"), "a"))
        val df = List(("1", "2"), ("1", "3"), ("2", "1"), ("2", "2")).toDF(List("a", "b"): _*)
        val features = DataSet(df, featureTypes).features

        features.oneToManyValues.toSet should ===(Set("a_1", "a_2"))
    }
  }

  "Given a two column feature and a data frame, Features" should {
    "give an indexed seq of containing the 4 one to many features as strings" in context {
      session =>
        import session.implicits._
        val featureTypes = List(OneToMany(List("a", "b"), "ab"))
        val df = List(("1", "2"), ("1", "3"), ("2", "1"), ("2", "2")).toDF(List("a", "b"): _*)
        val features = DataSet(df, featureTypes).features

        features.oneToManyValues.toSet should ===(Set("ab_1-2", "ab_1-3", "ab_2-1", "ab_2-2"))
    }
  }

  "Give a dataframe row, a Features" should {
    "return an appropriate feature vector when the values match the first feature value" in context {
      session =>
        import session.implicits._
        val featureTypes = List(OneToMany(List("a", "b"), "ab"))
        val features = Features(featureTypes, IndexedSeq("ab_1-2", "ab_1-3", "ab_2-1", "ab_2-2"))

        val row = List(("1", "2")).toDF(List("a", "b"): _*).collect().head

        FeatureVectors.featuresVectorForRow(row, features) should ===(Vectors.dense(1d, 0d, 0d, 0d))
    }

    "return an appropriate feature vector when the values match the third feature value" in context {
      session =>
        import session.implicits._
        val featureTypes = List(OneToMany(List("a", "b"), "ab"))
        val features = Features(featureTypes, IndexedSeq("ab_1-2", "ab_1-3", "ab_2-1", "ab_2-2"))

        val row = List(("2", "1")).toDF(List("a", "b"): _*).collect().head

        FeatureVectors.featuresVectorForRow(row, features) should ===(Vectors.dense(0d, 0d, 1d, 0d))
    }

    "return an appropriate feature vector for 2 one to many features" in context {
      session =>
        import session.implicits._
        val featureTypes = List(OneToMany(List("a", "b"), "ab"), OneToMany(List("z", "b"), "zb"))
        val features = Features(featureTypes, IndexedSeq("ab_1-2", "ab_1-3", "ab_2-1", "ab_2-2", "zb_s-1", "zb_t-1"))

        val row = List(("2", "1", "s")).toDF(List("a", "b", "z"): _*).collect().head

        FeatureVectors.featuresVectorForRow(row, features) should ===(Vectors.dense(0d, 0d, 1d, 0d, 1d, 0d))
    }
  }

  private def newSparkSession =
    SparkSession
      .builder
      .appName("DRT Analytics")
      .config("spark.master", "local")
      .getOrCreate()
}
