/**
  * Created by Davide Bertolotti on 13/08/2018.
  * // DB - DagSymb enhancements
  */
package org.apache.spark.deploy.control

import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._
import spray.json._
import DefaultJsonProtocol._

class HeuristicSymExControlUnlimited(conf: SparkConf) extends HeuristicControlUnlimited(conf) {
  
   /*Called with appJumboJson,validExecFlows list and jobId to return json of DAG profile
    * for 'worst case' execution.
    * DB - DagSymb enhancements
    */
  override def nextProfile(appJJ: JsValue, 
      valExFlows: java.util.ArrayList[Integer] = null, 
      jobId: Int = 0): JsValue = {
    var setP = appJJ.asJsObject.fields
    val stageId = if (valExFlows != null) 
                      setP(valExFlows.get(0).toString()).asJsObject.fields("0")
                      .asJsObject.fields("jobs")
                      .asJsObject.fields(jobId.toString())
                      .asJsObject.fields("stages")
                      .convertTo[List[Int]].sortWith((x, y) => x < y).apply(0)
    println("Next stage id: " + stageId)
    println("jobId: " + jobId)
    if (valExFlows != null) 
      setP = setP.filter({case (k,v) => valExFlows.exists(x => x == k.toInt)})
    
    /*  
    var wCaseProfId = setP.keys.toList.zip(setP.toList.map(
                      {case (k, v) => v.asJsObject.fields
                        .filter({case (k, v) => !v.asJsObject.fields("skipped").convertTo[Boolean]}) 
                        .count(_ => true)})
                      ).filter({case (id, ns) => ns == setP.toList.map(
                          {case (k, v) => v.asJsObject.fields
                            .filter({case (k, v) => !v.asJsObject.fields("skipped").convertTo[Boolean]})
                            .count(_ => true)}).max})(0)._1
                            */
     val findMax = (x: (String, Int), y: (String, Int)) => {
       if (x._2 > y._2){
         return x
       }
       else {
         return y;
       }
     }
     
     var wCaseProfId = setP.keys.toList.zip(setP.toList.map(
                      {case (k, v) => v.asJsObject.fields
                        .filter({case (k, v) => !v.asJsObject.fields("skipped").convertTo[Boolean]}) 
                        .count(_ => true)})).reduce(findMax)._1;
    println("Worst case json profile number: " + wCaseProfId)
    setP(wCaseProfId)
  }
}
