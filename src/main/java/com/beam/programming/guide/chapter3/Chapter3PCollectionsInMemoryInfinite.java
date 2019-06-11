package com.beam.programming.guide.chapter3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;


public class Chapter3PCollectionsInMemoryInfinite {
    public static void main(String[] args) {
        Instant baseTime = new Instant(0);
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        final TestStream<String> info = TestStream.<String>create(StringUtf8Coder.of())
                .advanceWatermarkTo(baseTime)
                .addElements(TimestampedValue.of("Hello", baseTime.plus(Duration.standardSeconds(10))))
                .addElements(TimestampedValue.of("World", baseTime.plus(Duration.standardSeconds(20))))
                .advanceWatermarkTo(baseTime.plus(Duration.standardSeconds(30)))
                .addElements(TimestampedValue.of("By", baseTime.plus(Duration.standardSeconds(30))))
                .addElements(TimestampedValue.of("World", baseTime.plus(Duration.standardSeconds(59))))
                .advanceWatermarkToInfinity();


        PCollection<String> pc = pipeline.apply(info).apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(30))));

        System.out.println(pc.isBounded());
        pc.apply(TextIO.write().to("src/main/resources/beam_output/output").withWindowedWrites().withNumShards(1));

        pipeline.run().waitUntilFinish();
    }
}
