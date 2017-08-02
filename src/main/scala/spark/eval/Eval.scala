package spark.eval

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import ml.shifu.shifu.core.DataPurifier
import ml.shifu.shifu.container.obj.EvalConfig
import ml.shifu.shifu.util.CommonUtils
import ml.shifu.shifu.container.obj.RawSourceData.SourceType
import ml.shifu.shifu.core.DataPurifier.ShifuMapContext
import ml.shifu.shifu.column.NSColumn
import ml.shifu.shifu.core.TreeModel
import org.apache.commons.lang.StringUtils
import org.apache.commons.jexl2.Expression
import org.apache.commons.jexl2.JexlEngine
import org.apache.commons.jexl2.JexlException
import org.apache.commons.jexl2.MapContext
import scala.collection.mutable.HashSet
import scala.collection.mutable.StringBuilder
import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer


object Eval {

    def main(args: Array[String] ) {
        val context = new SparkContext()
        val rawRdd = context.textFile("hdfs://stampy/user/website/wzhu1-test-data", 10)
        val purifiedRdd =  purifyData(rawRdd)
        val scoreRdd = eval(purifiedRdd)
        //purifiedRdd.saveAsTextFile("hdfs://stampy/user/website/wzhu1-test-result")
        scoreRdd.saveAsTextFile("hdfs://stampy/user/website/wzhu1-test-score")
    }

    def purifyData(rawRdd : RDD[String]) = {
       
        rawRdd.mapPartitionsWithIndex( (index, iterator) => {
            if(index == 0) {
                iterator.drop(1) 
            }

            val sourceType = SourceType.HDFS
            val modelConfigPath = "hdfs://stampy/user/website/wzhu1/ModelConfig.json"
            val columnConfigPath = "hdfs://stampy/user/website/wzhu1/ColumnConfig.json"
            val evalSetName = "Eval2"

            val modelConfig = CommonUtils.loadModelConfig(modelConfigPath, sourceType)
            val columnConfig = CommonUtils.loadColumnConfigList(columnConfigPath, sourceType)

            val evalConfig = modelConfig.getEvalConfigByName(evalSetName)
            val dataPurifier = new DataPurifier(evalConfig)
            val result = iterator.filter( 
                line => {
                   val result = dataPurifier.isFilterOut(line)
                   result match {
                        case value : java.lang.Boolean => Boolean.unbox(value)
                        case _ => false
                   }
                }
            )
            result
        })
    }

    def eval(filteredRdd : RDD[String]) : RDD[Any] = {
        filteredRdd.mapPartitions( iterator => {
            
            val sourceType = SourceType.HDFS
            val modelConfigPath = "hdfs://stampy/user/website/wzhu1/ModelConfig.json"
            val columnConfigPath = "hdfs://stampy/user/website/wzhu1/ColumnConfig.json"
            val evalSetName = "Eval2"

            val modelConfig = CommonUtils.loadModelConfig(modelConfigPath, sourceType)
            val columnConfig = CommonUtils.loadColumnConfigList(columnConfigPath, sourceType)

            val evalConfig = modelConfig.getEvalConfigByName(evalSetName)
            val models = CommonUtils.loadBasicModels(modelConfig, evalConfig, evalConfig.getDataSet.getSource, evalConfig.getGbtConvertToProb)
            val headers = CommonUtils.getFinalHeaders(evalConfig)
            val resultSet = new HashSet[String]
            while(iterator.hasNext) {
                val line = iterator.next
                val inputMap = new java.util.HashMap[String, Object]
                val fields = CommonUtils.split(line, evalConfig.getDataSet.getDataDelimiter)
                if(fields != null && fields.length == headers.length) {
                    for(i <- 0 until fields.length) {
                       if(fields(i) == null) {
                            inputMap.put(headers(i), "")
                       } else {
                            inputMap.put(headers(i), fields(i))
                       }
                    }
                    if(!inputMap.isEmpty()) {
                       val modelResultStr = new StringBuilder
                       if(StringUtils.isNotBlank(evalConfig.getDataSet.getWeightColumnName)) {
                            modelResultStr ++= inputMap.get(evalConfig.getDataSet.getWeightColumnName).toString
                       } else {
                            modelResultStr ++= "1.0"
                       }
                       modelResultStr ++= "|"
                       var modelResults = new ArrayBuffer[Double]
                       for(model <- JavaConverters.asScalaBufferConverter(models).asScala) {
                            modelResults = model match {
                                case tm : TreeModel => {
                                    val independentTreeModel = tm.getIndependentTreeModel
                                    val score = independentTreeModel.compute(inputMap)
                                    modelResults :+ score(0) 
                                }
                                case _ => modelResults
                           }
                       }
                       
                       val max = modelResults.max
                       val min = modelResults.min
                       val avg = modelResults.sum / modelResults.size
                       val median = modelResults(modelResults.size / 2)

                       modelResultStr ++= avg.toString() ++= "|"
                       modelResultStr ++= max.toString() ++= "|"
                       modelResultStr ++= min.toString() ++= "|"
                       modelResultStr ++= median.toString()

                       for(score <- modelResults) {
                            modelResultStr ++= "|" ++= score.toString()
                       }
                       for(metaColumn <- JavaConverters.asScalaBufferConverter(evalConfig.getAllMetaColumns(modelConfig)).asScala) {
                            modelResultStr ++= "|" ++= inputMap.get(metaColumn).toString
                       }
                       resultSet += modelResultStr.toString   
                    }
                }
            }
            resultSet.iterator
            
            
        })

    }
}
