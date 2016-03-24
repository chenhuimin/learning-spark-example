//package com.oreilly.learningsparkexamples.scala
//
//import scala.reflect.ClassTag
//import scala.reflect.runtime.universe._
//
////import scala.Predef._
///**
// * Created by user on 2016/3/4.
// */
//object TypeTagTest {
//  def paramInfo[T: TypeTag](x: T): Unit = {
//    val targs = implicitly[TypeTag[T]].tpe match {
//      case TypeRef(_, _, args) => args
//    }
//    println(s"type of $x has type arguments $targs")
//  }
//
//  def paramInfo2[T: ClassTag](x: T): Unit = {
//    val targs = classOf[T]
//    println(s"type of $x has type arguments $targs")
//  }
//
//  def paramInfo3[T](x: T): Unit = {
//    val targs = classOf[T]
//    println(s"type of $x has type arguments $targs")
//  }
//
//  def main(args: Array[String]) {
//  //  paramInfo2(42);
//    paramInfo2(List(1, 2))
//    paramInfo3(List(1, 2))
//
//  }
//
//}
