package com.scalapenos.riak

import java.util.UUID.randomUUID
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import akka.actor._
import akka.testkit._
import com.typesafe.config.{ Config, ConfigFactory }
import org.specs2.mutable._
import org.specs2.execute.{ Failure, FailureException }
import org.specs2.specification.StandardFragments.{ Backtab, Br }
import org.specs2.specification.{ Fragment, Fragments, Step, Text }
import org.specs2.time.NoTimeConversions

trait AkkaActorSystemSpecification extends Specification with NoTimeConversions {
  implicit val defaultSystem: ActorSystem = createActorSystem()

  implicit class PimpedFuture[T](f: Future[T]) {
    def await: T = Await.result(f, 5.seconds)
  }

  def failTest(msg: String) = throw FailureException(Failure(msg))

  lazy val actorSystems: ConcurrentHashMap[String, ActorSystem] = new ConcurrentHashMap[String, ActorSystem]()

  /* Add a final step to the list of test fragments that shuts down the actor system. */
  override def map(fs: => Fragments): Fragments =
    super.map(fs).add(Step(actorSystems.values().asScala.foreach(TestKit.shutdownActorSystem(_))))

  protected def createActorSystem(customConfig: Option[Config] = None): ActorSystem = {
    val systemName = s"tests-${randomUUID()}"
    val system = customConfig match {
      case Some(config) => ActorSystem(systemName, config)
      case None         => ActorSystem(systemName)
    }
    actorSystems.put(systemName, system)
    system
  }

  protected def decorateWith(fs: => Fragments)(text: String, block: => Unit): Seq[Fragment] = {
    Seq(Br(), Br(), Text(text), Step(block)) ++ fs.middle :+ Backtab(1)
  }

  protected def createRiakClient(enableCompression: Boolean): RiakClient = {
    val config = ConfigFactory.parseString(
      s"""
         |{
         | riak {
         |   enable-http-compression = $enableCompression
         |   add-client-id-header = false
         |   ignore-tombstones = true
         | }
         |}
      """.stripMargin).withFallback(ConfigFactory.load())

    RiakClient(createActorSystem(Some(config)))
  }
}

abstract class RiakClientSpecification extends AkkaActorSystemSpecification with Before {
  var client: RiakClient = _

  def before {
    client = RiakClient(defaultSystem)
  }

  skipAllUnless(RiakClient(defaultSystem).ping.await)

  private def specsWithParametrizedCompression(fs: => Fragments): Fragments = {
    Seq(true, false).map { enableCompression =>
      val compressionCaseText = s"When compression is ${if (enableCompression) "enabled" else "disabled"} in"

      fs.copy(middle = decorateWith(fs)(text = compressionCaseText, block = {
        client = createRiakClient(enableCompression)
      }))
    }.reduce(_ ^ _)
  }

  override def map(fs: => Fragments): Fragments = super.map(specsWithParametrizedCompression(fs))
}

trait RandomKeySupport {
  import java.util.UUID._

  def randomKey: String = randomUUID().toString
}

trait RandomBucketSupport { self: RandomKeySupport =>

  def randomBucket(client: RiakClient): RiakBucket = client.bucket("riak-bucket-tests-" + randomKey)
}
