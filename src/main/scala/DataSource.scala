package org.template.ecommercerecommendation

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceEvalParams(kFold: Int, queryNum: Int)

case class DataSourceParams(
  appName: String,
  evalParams: Option[DataSourceEvalParams]) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, ActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // create a RDD of (entityID, User)
    val usersRDD: RDD[(String, User)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user"
    )(sc).map { case (entityId, properties) =>
      val user = try {
        User()
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" user ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, user)
    }.cache()

    // create a RDD of (entityID, Item)
    val itemsRDD: RDD[(String, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {
        // Assume categories is optional property of item.
        Item(categories = properties.getOpt[List[String]]("categories"))
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }.cache()

    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("view", "buy")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))(sc)
      .cache()

    val viewEventsRDD: RDD[ViewEvent] = eventsRDD
      .filter { event => event.event == "view" }
      .map { event =>
        try {
          ViewEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to ViewEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    val buyEventsRDD: RDD[BuyEvent] = eventsRDD
      .filter { event => event.event == "buy" }
      .map { event =>
        try {
          BuyEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to BuyEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    new TrainingData(
      users = usersRDD,
      items = itemsRDD,
      viewEvents = viewEventsRDD,
      buyEvents = buyEventsRDD
    )
  }

  override
  def readEval(sc: SparkContext)
      : Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {

    require(!dsp.evalParams.isEmpty, "Must specify evalParams")
    val evalParams = dsp.evalParams.get

    // create a RDD of (entityID, User)
    val usersRDD: RDD[(String, User)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user"
    )(sc).map { case (entityId, properties) =>
      val user = try {
        User()
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" user ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, user)
    }.cache()

    // create a RDD of (entityID, Item)
    val itemsRDD: RDD[(String, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {
        // Assume categories is optional property of item.
        Item(categories = properties.getOpt[List[String]]("categories"))
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }.cache()

    val eventsRDD: RDD[(Event,Long)] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("view", "buy")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))(sc).zipWithUniqueId.cache()

    val kFold = evalParams.kFold
    (0 until kFold).map { idx => {
      logger.info(s"kFold: ${idx}.")

      val trainingEventsRDD: RDD[Event] = eventsRDD.filter(_._2 % kFold != idx).map(_._1)
      logger.info(s"trainingEventsRDD count: ${trainingEventsRDD.count()}.")

      val testEventsRDD: RDD[Event] = eventsRDD.filter(_._2 % kFold == idx).map(_._1)
      logger.info(s"testEventsRDD count: ${testEventsRDD.count()}.")

      val trainingViewEventsRDD: RDD[ViewEvent] = trainingEventsRDD
        .filter { event => event.event == "view" }
        .map { event =>
          try {
            ViewEvent(
              user = event.entityId,
              item = event.targetEntityId.get,
              t = event.eventTime.getMillis
            )
          } catch {
            case e: Exception =>
              logger.error(s"Cannot convert ${event} to ViewEvent." +
                s" Exception: ${e}.")
              throw e
          }
        }
      logger.info(s"trainingViewEventsRDD count: ${trainingViewEventsRDD.count()}.")

      val trainingBuyEventsRDD: RDD[BuyEvent] = trainingEventsRDD
        .filter { event => event.event == "buy" }
        .map { event =>
          try {
            BuyEvent(
              user = event.entityId,
              item = event.targetEntityId.get,
              t = event.eventTime.getMillis
            )
          } catch {
            case e: Exception =>
              logger.error(s"Cannot convert ${event} to BuyEvent." +
                s" Exception: ${e}.")
              throw e
          }
        }
      logger.info(s"trainingBuyEventsRDD count: ${trainingBuyEventsRDD.count()}.")

      val testViewEventsRDD: RDD[ViewEvent] = testEventsRDD
        .filter { event => event.event == "view" }
        .map { event =>
          try {
            ViewEvent(
              user = event.entityId,
              item = event.targetEntityId.get,
              t = event.eventTime.getMillis
            )
          } catch {
            case e: Exception =>
              logger.error(s"Cannot convert ${event} to ViewEvent." +
                s" Exception: ${e}.")
              throw e
          }
        }
      logger.info(s"testViewEventsRDD count: ${testViewEventsRDD.count()}.")

      val testBuyEventsRDD: RDD[BuyEvent] = testEventsRDD
        .filter { event => event.event == "buy" }
        .map { event =>
          try {
            BuyEvent(
              user = event.entityId,
              item = event.targetEntityId.get,
              t = event.eventTime.getMillis
            )
          } catch {
            case e: Exception =>
              logger.error(s"Cannot convert ${event} to BuyEvent." +
                s" Exception: ${e}.")
              throw e
          }
        }
      logger.info(s"testBuyEventsRDD count: ${testBuyEventsRDD.count()}.")


      val viewbuy = Set("view","buy")
      val testingUsers = testEventsRDD.filter{ event => viewbuy.contains(event.event) }.map(event=>(event.entityId)).distinct
      logger.info(s"testingUsers count: ${testingUsers.count()}.")

      val is1 = testBuyEventsRDD.map(ev=>((ev.user,ev.item),10.0))
      val is2 = testViewEventsRDD.map(ev=>((ev.user,ev.item),1.0))
      val is = is1.union(is2).reduceByKey(_+_)
      
      //val trainingItemScores = trainingBuyEventsRDD.map(ev=>(ev.user, new ItemScore(ev.item, 1.0))).groupByKey().map(x=>(x._1, x._2.toArray)).collect
      val testItemScores = is.map(ev=>(ev._1._1, new ItemScore(ev._1._2, ev._2))).groupByKey().map(x=>(x._1, x._2.toArray)).collect
      logger.info(s"testItemScores size: ${testItemScores.size}.")

      (new TrainingData(users = usersRDD,items = itemsRDD,viewEvents = trainingViewEventsRDD,buyEvents = trainingBuyEventsRDD),new EmptyEvaluationInfo(),testingUsers.map{user => (Query(user=user, num=evalParams.queryNum,categories=None,whiteList=None,blackList=None), ActualResult(testItemScores.filter(_._1==user).flatMap(_._2) ))})
    }}
  }
}

case class User()

case class Item(categories: Option[List[String]])

case class ViewEvent(user: String, item: String, t: Long)

case class BuyEvent(user: String, item: String, t: Long)

class TrainingData(
  val users: RDD[(String, User)],
  val items: RDD[(String, Item)],
  val viewEvents: RDD[ViewEvent],
  val buyEvents: RDD[BuyEvent]
) extends Serializable {
  override def toString = {
    s"users: [${users.count()} (${users.take(2).toList}...)]" +
    s"items: [${items.count()} (${items.take(2).toList}...)]" +
    s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)" +
    s"buyEvents: [${buyEvents.count()}] (${buyEvents.take(2).toList}...)"
  }
}
