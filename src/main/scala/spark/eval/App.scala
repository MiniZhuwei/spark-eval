package spark.eval

import ml.shifu.shifu.core.DataPurifier
import ml.shifu.shifu.container.obj.EvalConfig
import ml.shifu.shifu.util.CommonUtils
import ml.shifu.shifu.container.obj.RawSourceData.SourceType
import org.apache.commons.jexl2.Expression
import org.apache.commons.jexl2.JexlEngine
import org.apache.commons.jexl2.JexlException
import org.apache.commons.jexl2.MapContext
import ml.shifu.shifu.core.DataPurifier.ShifuMapContext
import ml.shifu.shifu.column.NSColumn
import scala.io.Source


object App {

    def main(args: Array[String] ) {

        val evalConfig = loadConfig
        val dataPurifier = new DataPurifier(evalConfig)
        val iterator = Source.fromFile("/Users/wzhu1/shifu-test/data.txt").getLines
        val result = iterator.filter(
             
            rawInput => {
                val result = dataPurifier.isFilterOut(rawInput)
                val converted = result match {
                    case value : java.lang.Boolean => Boolean.unbox(value)
                    case _ => false
                }
                converted
            }
        )
        while(result.hasNext) {
            println(result.next)
        }
    }
              
    def loadConfig : EvalConfig = {
        val sourceType = SourceType.LOCAL
        val modelConfigPath = "/Users/wzhu1/shifu-test/camcsv6/ModelConfig.json"
        val columnConfigPath = "/Users/wzhu1/shifu-test/camcsv6/ColumnConfig.json"
        val evalSetName = "Eval2"

        val modelConfig = CommonUtils.loadModelConfig(modelConfigPath, sourceType)
        val columnConfig = CommonUtils.loadColumnConfigList(columnConfigPath, sourceType)

        modelConfig.getEvalConfigByName(evalSetName)
    }


}
