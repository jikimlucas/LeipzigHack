package eu.streamline.hackathon.flink.scala.job

import java.util.Date

import eu.streamline.hackathon.common.data.GDELTEvent
import eu.streamline.hackathon.flink.operations.GDELTInputFormat
import org.apache.flink.api.common.functions.FoldFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SocketClientSink
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector


object FlinkScalaJob {

  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = "/Users/jlucas/Documents/leipzig/180-days.csv"
    val country = parameters.get("country", "USA")

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

    .fold(
        0.0,
       // "",
      //Tuple2(0.0, 0),
        new FoldFunction[GDELTEvent, Double] {
          override def fold(accumulator: Double, value: GDELTEvent) = {
       // override def fold(accumulator: Tuple2, value: GDELTEvent) = {
           // accumulator + value.avgTone
            accumulator
          }
        },
        new WindowFunction[Double, (String), String, TimeWindow] {
          override def apply(key: String,
                             window: TimeWindow,
                             input: Iterable[Double],
                             //out: Collector[(String, Double, String)]
                             out: Collector[(String)]
                              ): Unit = {
            val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
            val formated_date = format.format(new Date(window.getStart))
            val output = key + "|" + input.head + "|" + formated_date + "\n"
           // out.collect((key, input.head, formated_date))
            out.collect(output)
          }
        }
    )
    .writeToSocket("localhost",5555, new SimpleStringSchema())
    //  writeToSocket("localhost", 9000, new SimpleStringSchema )

/**
   val consumer_stream = env.socketTextStream("localhost", 5555, '\n')

    env.socketTextStream("localhost", 5555)
      .map(filtered_source => { println(filtered_source); filtered_source })
      .addSink(new SocketClientSink[String]("localhost", 5555, new SimpleStringSchema))
**/
    //filtered_source.print();

    //

    env.execute("Flink Scala GDELT Analyzer")

  }

}
