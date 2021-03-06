package nodescala

import scala.language.postfixOps
import scala.util.{Try, Success, Failure}
import scala.collection._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import org.scalatest._
import NodeScala._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  case class MyTestException() extends Exception

  test("Future.always should always be completed") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }

  test("Future.never should never be completed") {
    val never = Future.never[Int]

    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("Future.all should return successful results, in right order") {
    val if1: Future[Int] = Future { Thread.sleep(300); 100 }
    val if2: Future[Int] = Future { Thread.sleep(200); 200 }
    val if3: Future[Int] = Future { Thread.sleep(100); 300 }

    val all: Future[List[Int]] = Future.all[Int](List(if2, if3, if1))
    val correctResult: List[Int] = List(200, 300, 100)

    assert(Await.result(all, 500 millis) == correctResult)
  }

  test("Future.all should fail if any given future fails") {
    val if1: Future[Int] = Future { Thread.sleep(300);
      throw new NoSuchElementException("Born to Fail") }
    val if2: Future[Int] = Future { Thread.sleep(200); 200 }
    val if3: Future[Int] = Future { Thread.sleep(100); 300 }

    val all: Future[List[Int]] = Future.all[Int](List(if2, if3, if1))

    try {
      Await.result(all, 500 millis)
      assert(false)
    } catch {
      case t: NoSuchElementException => // ok!
    }
  }

  test("Future.any should return first result to complete (here, success)") {
    val if1: Future[Int] = Future { Thread.sleep(300)
      throw new NoSuchElementException("Born to Fail") }
    val if2: Future[Int] = Future { Thread.sleep(200); 200 }
    val if3: Future[Int] = Future { Thread.sleep(100); 300 }

    val any: Future[Int] = Future.any[Int](List(if2, if3, if1))
    val correctResult: Int = 300

    assert(Await.result(any, 500 millis) == correctResult)
  }

  test("Future.any should return first result to complete (here, failure)") {
    val if1: Future[Int] = Future { Thread.sleep(300); 100 }
    val if2: Future[Int] = Future { Thread.sleep(200); 200 }
    val if3: Future[Int] = Future { Thread.sleep(100);
      throw new NoSuchElementException("Born to Fail") }

    val any: Future[Int] = Future.any[Int](List(if2, if3, if1))

    try {
      Await.result(any, 500 millis)
      assert(false)
    } catch {
      case t: NoSuchElementException => // ok!
    }
  }

  test("Future.delay should first not complete, then complete") {
    val delay: Future[Unit] = Future.delay(250 millis)
    assert(!delay.isCompleted)
    Thread.sleep(500)
    assert(delay.isCompleted)
  }

  test("Future.run should let an indefinitely-running computation go until cancelled") {
    class TestVal(var v: String) {
      def set(newV: String) { this.synchronized { v = newV } }
    }
    val tv = new TestVal("init")

    val working = Future.run() { ct => Future {
      while (ct.nonCancelled) { tv.set("running") }
      tv.set("done")
    }}

    Future.delay(100 millis) onSuccess {
      case _ => assert(tv.v == "running")
    }

    Future.delay(200 millis) onSuccess {
      case _ => assert(tv.v == "running")
    }

    Future.delay(300 millis) onSuccess {
      case _ => working.unsubscribe() // sending cancellation
    }

    Future.delay(400 millis) onSuccess {
      case _ => assert(tv.v == "done")
    }
  }

  test("A future's now should yield result (or exeption) when it's completed") {
    val f1: Future[Int] = Future { Thread.sleep(100); 99 }
    val f2: Future[Int] = Future { Thread.sleep(100);
      throw new MyTestException() }

    Thread.sleep(200)

    assert(f1.now == 99)
    try {
      f2.now
      assert(false)
    } catch {
      case t: MyTestException => //ok!
    }
  }

  test("A future's now should throw an exception when it's not completed") {
    val f: Future[Int] = Future { Thread.sleep(100); 99 }
    try {
      f.now
      assert(false)
    } catch {
      case t: NoSuchElementException => // ok!
    }
  }

  test("A future's continueWith should do the continuation, as a future") {
    val f: Future[Int] = Future { Thread.sleep(100); 25 }
    def c(f: Future[Int]): String = {
      val i: Int = Await.result(f, 200 millis)
      (i * 2).toString + "!!!"
    }
    val cw: Future[String] = f.continueWith(c)
    assert(!cw.isCompleted)
    Thread.sleep(250)
    assert(cw.isCompleted)
    assert(Await.result(cw, 0 nanos) == "50!!!")
  }

  test("A future's continue should do the continuation, as a future") {
    val f: Future[Int] = Future { Thread.sleep(100); 99 }
    def c(r: Try[Int]): String = r match {
      case Success(i) => (i * 2).toString + "!!!"
      case _ => "Did not receive valid int."
    }
    val cont: Future[String] = f.continue(c)
    assert(!cont.isCompleted)
    Thread.sleep(250)
    assert(cont.isCompleted)
    assert(Await.result(cont, 0 nanos) == "198!!!")
  }

  test("A future's continue should handle exceptions thrown by continuation func") {
    val f: Future[Int] = Future { Thread.sleep(100); 99 }
    def c(r: Try[Int]): String = throw new MyTestException()
    val cont: Future[String] = f.continue(c)

    assert(!cont.isCompleted)
    Thread.sleep(250)
    assert(cont.isCompleted)
    try {
      Await.result(cont, 0 nanos)
      assert(false)
    } catch {
      case t: MyTestException => //ok!
    }
  }

  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }
  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 1 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }

}




