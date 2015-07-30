package org.template.ecommercerecommendation

import io.prediction.controller.IEngineFactory
import io.prediction.controller.Engine

case class Query(
  user: Int,
  num: Int,
  categories: Option[Set[String]],
  whiteList: Option[Set[Int]],
  blackList: Option[Set[Int]]
) extends Serializable

case class PredictedResult(
  itemScores: Array[ItemScore]
) extends Serializable

case class ItemScore(
  item: Int,
  score: Double
) extends Serializable

object ECommerceRecommendationEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("ecomm" -> classOf[ECommAlgorithm]),
      classOf[Serving])
  }
}
