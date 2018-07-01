package eu.streamline.hackathon.flink.scala.job

import java.util.Date

import eu.streamline.hackathon.common.data.GDELTEvent
import eu.streamline.hackathon.flink.operations.GDELTInputFormat
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions._
import org.apache.flink.api.common.state.{AggregatingStateDescriptor, FoldingStateDescriptor, ListStateDescriptor, ReducingStateDescriptor}



object FlinkScalaJob {

  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = "/Users/jlucas/Documents/leipzig/180-days.csv"
   // val country = parameters.get("country", "USA")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    implicit val typeInfo = createTypeInformation[GDELTEvent]
    implicit val dateInfo = createTypeInformation[Date]



    val source = env
      .readFile[GDELTEvent](new GDELTInputFormat(new Path(pathToGDELT)), pathToGDELT)
      .setParallelism(1)

    val filtered_source = source.filter((event: GDELTEvent) => {
        event.actor1Code_countryCode != null
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[GDELTEvent](Time.seconds(0)) {
      override def extractTimestamp(element: GDELTEvent): Long = {
        element.dateAdded.getTime
      }
    }).keyBy((event: GDELTEvent) => {
      event.actor1Code_countryCode
    }).window(TumblingEventTimeWindows.of(Time.days(1)))
    .aggregate(new SumAggregate2(), new SumWindowFunction())
  //.writeAsText("/Users/jlucas/Documents/leipzig/filtered.csv")
  // .writeToSocket("localhost",5555, new SimpleStringSchema())

    filtered_source.print()


    env.execute("Flink Scala GDELT Analyzer")

  }

  class SumAggregate extends AggregateFunction[GDELTEvent, (Double, Integer), (Double, Integer)] {

    override def createAccumulator(): (Double, Integer) = (0.0, 0)

    override def merge(a: (Double, Integer), b: (Double, Integer)): (Double, Integer) = (a._1+b._1, a._2 + b._2)

    override def getResult(accumulator: (Double, Integer)): (Double, Integer) = accumulator

    override def add(value: GDELTEvent, accumulator: ( Double, Integer))  = (
      accumulator._1 + value.avgTone, accumulator._2 +1)
  }

  class SumAggregate2 extends AggregateFunction[GDELTEvent, (Double, Integer, Double,String), (Double, Integer,Double, String)] {

    override def createAccumulator(): (Double, Integer, Double, String) = (0.0, 0, 0.0,"")

    override def merge(a: (Double, Integer, Double,String), b: (Double, Integer,Double,String)): (Double, Integer,Double,String)
    = (a._1+b._1, a._2 + b._2, a._3, a._4)

    override def getResult(accumulator: (Double, Integer, Double,String)): (Double, Integer, Double,String) = accumulator

    override def add(value: GDELTEvent, accumulator: ( Double, Integer, Double, String))  = (
      accumulator._1 + value.avgTone, accumulator._2 +1,
      if( Math.abs(accumulator._3) < Math.abs(value.avgTone)  ) value.avgTone else accumulator._3,
      if( Math.abs(accumulator._3) <  Math.abs(value.avgTone)  ) value.eventCode else accumulator._4
      )
  }

  class SumWindowFunction
    extends WindowFunction[(Double, Integer, Double, String), (String), String, TimeWindow] {

    override def apply(
                        key: String,
                        window: TimeWindow,
                        input: Iterable[(Double, Integer, Double, String)],
                       // out: Collector[(String, Double, Integer, String)]): Unit = {
                        out: Collector[(String)]): Unit = {
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val formated_date = format.format(new Date(window.getStart))

      input.foreach(e => {
        val output = key + "|" + e._1 + "|" + e._2 + "|" +  e._3 + "|"  + e._4 + "|" + formated_date + "\n"
        out.collect(
          (output)
        )
      }
      )
    }
  }
}
