package part3_graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{BidiShape, ClosedShape}
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}
object BidirectionalFlows extends App {
  implicit val system = ActorSystem("bi-directional-flows")

  /*
  Example: cryptography
   */

  def encrypt(n: Int)(string: String) = string.map(c => (c + n).toChar)
  def decrypt(n: Int)(string: String) = string.map(c => (c - n).toChar)

  println(encrypt(3)("Akka"))
  println(decrypt(3)("Dnnd"))

  // bidiFlow
  val bidiCryptoStaticGraph = GraphDSL.create() { implicit builder =>
    val encryptionFlowShape = builder.add(Flow[String].map(encrypt(3)))
    val decryptionFlowShape = builder.add(Flow[String].map(decrypt(3)))

//    BidiShape(encryptionFlowShape.in, encryptionFlowShape.out, decryptionFlowShape.in, decryptionFlowShape.out)
    BidiShape.fromFlows(encryptionFlowShape, decryptionFlowShape)
  }

  val unencryptedStrings = List("Akka", "is", "awesome", "testing")
  val unencryptedSource = Source(unencryptedStrings)

  val encryptedSource = Source(unencryptedStrings.map(encrypt(3)))

  val cryptoBidiGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val unencryptedSourceShape = builder.add(unencryptedSource)
      val encryptedSourceShape = builder.add(encryptedSource)

      val bidi = builder.add(bidiCryptoStaticGraph)

      val encryptedSinkShape = builder.add(
        Sink.foreach[String](string => println(s"Encrypted: $string"))
      )
      val decryptedSinkShape = builder.add(
        Sink.foreach[String](string => println(s"Decrypted: $string"))
      )

      unencryptedSourceShape ~> bidi.in1; bidi.out1 ~> encryptedSinkShape
//      encryptedSourceShape ~> bidi.in2; bidi.out2 ~> decryptedSinkShape
      decryptedSinkShape <~ bidi.out2; bidi.in2 <~ encryptedSourceShape

      ClosedShape
    }
  )
  cryptoBidiGraph.run()
  /*
  encrypt/decrypt
  encode/decode
  serialize/de-serialize
   */
}
