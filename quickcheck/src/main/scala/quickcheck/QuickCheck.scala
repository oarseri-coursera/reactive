package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {
  val minEntry = -50
  val maxEntry = 50
  val minHeapSize = 0
  val maxHeapSize = 10
  val subMinEntry = minEntry - 1

  def equivalentP(h1: H, h2: H) = {
    def listing(h: H) = {
      var hc = h
      var l: List[Int] = List()
      while (!isEmpty(hc)) {
        l = findMin(hc)::l
        hc = deleteMin(hc)
      }
      l
    }
    //println("h1" + listing(h1))
    //println("h2" + listing(h2))
    listing(h1) == listing(h2)
  }

  property("'equivalentP' works correctly") = forAll { h: H =>
    equivalentP(h,h) && true
    !equivalentP(h,insert(maxEntry,h))
  }

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("add duplicate min") = forAll { h: H =>
    findMin(insert(findMin(h), h)) == findMin(h)
  }

  property("correct (known) min after two inserts") = forAll { h: H =>
    findMin(insert(subMinEntry,insert(maxEntry, h))) == subMinEntry
  }

  property("min rises (or stays same) after two deletes") = forAll { h: H =>
    val hp = insert(maxEntry, (insert(maxEntry, h))) // size at least 2
    val hpp = deleteMin(hp)
    val hppp = deleteMin(hpp)
    ( findMin(hp) <= findMin(hpp) ) && ( findMin(hpp) <= findMin(hppp) )
  }

  property("inserting then removing (known) min is no-op") = forAll { h: H =>
    findMin(deleteMin(insert(subMinEntry,h))) == findMin(h)
  }

  property("min of merge is lesser of mins of merged") = forAll { (h1: H, h2: H) =>
    val m1 = findMin(h1)
    val m2 = findMin(h2)
    val m = findMin(meld(h1,h2))
    m == { if (m1 < m2) m1 else m2 }
  }

  property("merging is associative") = forAll { (h1: H, h2: H, h3: H) =>
    val ha = meld(meld(h1, h2), h3)
    val hb = meld(h1, meld(h2, h3))
    equivalentP(ha,hb)
  }

  property("self merge yields duplicate min") = forAll { h: H =>
    val m = findMin(h)
    val hh = meld(h,h)
    ( findMin(hh) == m ) && ( findMin(deleteMin(hh)) == m )

  }

  lazy val genHeap: Gen[H] = {
    val heapSize = Gen.choose(minHeapSize, maxHeapSize)
    val entries = Gen.listOfN(maxHeapSize, Gen.choose(minEntry, maxEntry))
    for { es <- entries } yield es.foldLeft(empty) { (acc,e) => insert(e,acc) }
  }

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
