package uk.gov.homeoffice.drt.analytics.actors

import org.specs2.mutable.Specification
import uk.gov.homeoffice.drt.prediction.Feature.{OneToMany, Single}
import uk.gov.homeoffice.drt.prediction.{FeaturesWithOneToManyValues, ModelAndFeatures, RegressionModel, TouchdownModelAndFeatures}

class ModelAndFeaturesConversionTest extends Specification {
  "Given a ModelAndFeatures class" >> {
    "I should be able to serialise and deserialise it back to its original form" >> {
      val model = RegressionModel(Seq(1, 2, 3), -1.45)
      val features = FeaturesWithOneToManyValues(List(OneToMany(List("a", "b"), "_a"), Single("c")), IndexedSeq("aa", "bb", "cc"))
      val modelAndFeatures = ModelAndFeatures(model, features, TouchdownModelAndFeatures.targetName, 100, 10.1)

      val serialised = ModelAndFeaturesConversion.modelAndFeaturesToMessage(modelAndFeatures, 0L)

      val deserialised = ModelAndFeaturesConversion.modelAndFeaturesFromMessage(serialised)

      deserialised === modelAndFeatures
    }
  }
  "Given a TouchdownModelAndFeatures class" >> {
    "I should be able to serialise and deserialise it back to its original form" >> {
      val model = RegressionModel(Seq(1, 2, 3), -1.45)
      val features = FeaturesWithOneToManyValues(List(OneToMany(List("a", "b"), "_a"), Single("c")), IndexedSeq("aa", "bb", "cc"))
      val modelAndFeatures = TouchdownModelAndFeatures(model, features, 100, 10.1.toInt)

      val serialised = ModelAndFeaturesConversion.modelAndFeaturesToMessage(modelAndFeatures, 0L)

      val deserialised = ModelAndFeaturesConversion.modelAndFeaturesFromMessage(serialised)

      deserialised === modelAndFeatures
    }
  }
  "Two ModelAndFeatures with different values should not be equal" >> {
    val features = FeaturesWithOneToManyValues(List(OneToMany(List("a", "b"), "_a"), Single("c")), IndexedSeq("aa", "bb", "cc"))
    val model1 = RegressionModel(Seq(1, 2, 3), -1.45)
    val model2 = RegressionModel(Seq(2, 3, 4), 0.20)
    val modelAndFeatures1 = TouchdownModelAndFeatures(model1, features, 100, 10.1.toInt)
    val modelAndFeatures2 = TouchdownModelAndFeatures(model2, features, 100, 20.1.toInt)

    val areEqual = (modelAndFeatures1, modelAndFeatures2) match {
      case (m1: ModelAndFeatures, m2: ModelAndFeatures) => m1 == m2
    }

    areEqual === false
  }
}
