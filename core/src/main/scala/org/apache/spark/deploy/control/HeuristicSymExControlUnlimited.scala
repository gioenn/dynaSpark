/**
  * Created by Davide Bertolotti on 13/08/2018.
  * // DB - DagSymb enhancements
  */
package org.apache.spark.deploy.control

import org.apache.spark.SparkConf
import spray.json._

class HeuristicSymExControlUnlimited(conf: SparkConf) extends HeuristicControlUnlimited(conf) {
  
   /*Called with appJumboJson and validExecFlows list to return json DAG profile
    * for the 'worst case' execution.
    * DB - DagSymb enhancements
    */
  def nextProfile(appJJ: JsValue, 
      valExFlows: java.util.ArrayList[Integer] = null, 
      jobId: Int = 0): JsValue = {
    var setP = appJJ.asJsObject.fields
    val stageId = if (valExFlows != null) 
                      setP(valExFlows.get(0).toString()).asJsObject.fields("0")
                      .asJsObject.fields("jobs")
                      .asJsObject.fields(jobId.toString())
                      .asJsObject.fields("stages")
                      .convertTo[List[Int]].apply(0)
                  else 0
    println("Next stage id: " + stageId)
    println("jobId: " + jobId)
    if (valExFlows != null) 
      setP = setP.filter({case (k,v) => valExFlows.exists(x => x == k.toInt)})
    var wCaseProfId = setP.keys.toList.zip(setP.toList.map(
                      {case (k, v) => v.asJsObject.fields.count(_ => true)})
                      ).filter({case (id, ns) => ns == setP.toList.map(
                          {case (k, v) => v.asJsObject.fields.count(_ => true)}).max})(0)._1
    println("Worst case json profile number: " + wCaseProfId)
    setP(wCaseProfId)
  }
}
