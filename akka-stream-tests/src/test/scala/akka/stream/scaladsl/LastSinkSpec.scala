/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import org.reactivestreams.Subscriber

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure

import akka.stream.ActorMaterializer
import akka.stream.ActorMaterializerSettings
import akka.stream.testkit._
import akka.stream.testkit.Utils._

class LastSinkSpec extends AkkaSpec with ScriptedTest {

  val settings = ActorMaterializerSettings(system)

  implicit val materializer = ActorMaterializer(settings)

  "A Flow with Sink.head" must {

    "yield the last value" in assertAllStagesStopped {
      Await.result(Source(1 to 42).map(identity).runWith(Sink.last), 100.millis) should be(42)
    }

    "yield the last value when actively constructing" in {
      val p = TestPublisher.manualProbe[Int]()
      val f = Sink.head[Int]
      val s = Source.subscriber[Int]
      val (subscriber, future) = s.toMat(f)(Keep.both).run()

      p.subscribe(subscriber)
      val proc = p.expectSubscription()
      proc.expectRequest()
      proc.sendNext(42)
      proc.sendComplete()
      Await.result(future, 100.millis) should be(42)
    }

    "yield the first error" in assertAllStagesStopped {
      val p = TestPublisher.manualProbe[Int]()
      val f = Source(p).runWith(Sink.last)
      val proc = p.expectSubscription()
      proc.expectRequest()
      val ex = new RuntimeException("ex")
      proc.sendError(ex)
      Await.ready(f, 100.millis)
      f.value.get should be(Failure(ex))
    }

    "yield NoSuchElementException for empty stream" in assertAllStagesStopped {
      val p = TestPublisher.manualProbe[Int]()
      val f = Source(p).runWith(Sink.last)
      val proc = p.expectSubscription()
      proc.expectRequest()
      proc.sendComplete()
      Await.ready(f, 100.millis)
      f.value.get match {
        case Failure(e: NoSuchElementException) ⇒ e.getMessage should be("last of empty stream")
        case x                                  ⇒ fail("expected NoSuchElementException, got " + x)
      }
    }

  }
  "A Flow with Sink.lastOption" must {

    "yield the last value" in assertAllStagesStopped {
      Await.result(Source(1 to 42).map(identity).runWith(Sink.lastOption), 100.millis) should be(Some(42))
    }

    "yield the first error" in assertAllStagesStopped {
      val p = TestPublisher.manualProbe[Int]()
      val f = Source(p).runWith(Sink.last)
      val proc = p.expectSubscription()
      proc.expectRequest()
      val ex = new RuntimeException("ex")
      proc.sendError(ex)
      Await.ready(f, 100.millis)
      f.value.get should be(Failure(ex))
    }

    "yield None for empty stream" in assertAllStagesStopped {
      val p = TestPublisher.manualProbe[Int]()
      val f = Source(p).runWith(Sink.lastOption)
      val proc = p.expectSubscription()
      proc.expectRequest()
      proc.sendComplete()
      Await.result(f, 100.millis) should be(None)
    }

  }

}
