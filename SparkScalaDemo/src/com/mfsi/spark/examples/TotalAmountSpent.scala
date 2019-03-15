package com.mfsi.spark.examples

import org.apache.log4j._
import org.apache.spark.SparkContext


object TotalAmountSpent {
  
  
  def main(args:Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc= new SparkContext("local[*]","TotatAmountSpent")
    val lines = sc.textFile("../SparkScala/customer-orders.csv")
    lines.foreach(println)
    val customersAmountSpent = lines.map(x=>(x.split(",")(0).toInt,x.split(",")(2).toFloat));
    val totalAmountSpentByCustom= customersAmountSpent.reduceByKey((x,y)=>x+y)
    val flipped= totalAmountSpentByCustom.map(x=>(x._2,x._1))
    val sorted = flipped.sortByKey()
    val totalAmountSpent=sorted.collect;
    
    println("Total amount  spent by each customer")
    for(x <- totalAmountSpent)
    {
      val amount= x._1
      val cust=x._2
      println(s"$amount Dollar spent by customer : $cust")
    }
      
  }
  
}