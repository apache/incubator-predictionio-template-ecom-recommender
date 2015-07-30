package org.template.ecommercerecommendation

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class PreparatorTest
  extends FlatSpec with EngineTestSparkContext with Matchers {

  val preparator = new Preparator()
  val users = Map(
    0 -> User(),
    1 -> User()
  )

  val items = Map(
    0 -> Item(categories = Some(List("c0", "c1"))),
    1 -> Item(categories = None)
  )

  val view = Seq(
    ViewEvent(0, 0, 1000010),
    ViewEvent(0, 1, 1000020),
    ViewEvent(1, 1, 1000030)
  )

  val buy = Seq(
    BuyEvent(0, 0, 1000020),
    BuyEvent(0, 1, 1000030),
    BuyEvent(1, 1, 1000040)
  )

  // simple test for demonstration purpose
//  "Preparator" should "prepare PreparedData" in {
//
//    val trainingData = new TrainingData(
//      users = sc.parallelize(users.toSeq),
//      items = sc.parallelize(items.toSeq),
//      viewEvents = sc.parallelize(view.toSeq),
//      buyEvents = sc.parallelize(buy.toSeq)
//    )
//
//    val preparedData = preparator.prepare(sc, trainingData)
//
//    preparedData.users.collect should contain theSameElementsAs users
//    preparedData.items.collect should contain theSameElementsAs items
//    preparedData.viewEvents.collect should contain theSameElementsAs view
//    preparedData.buyEvents.collect should contain theSameElementsAs buy
//  }
}
