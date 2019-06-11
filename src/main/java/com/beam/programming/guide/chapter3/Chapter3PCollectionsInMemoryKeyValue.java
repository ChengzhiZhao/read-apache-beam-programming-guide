package com.beam.programming.guide.chapter3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class Chapter3PCollectionsInMemoryKeyValue {

    static class doNothing extends DoFn<KV<Integer, String>, String> {
        @ProcessElement
        public void processElement(@Element KV<Integer, String> line, OutputReceiver<String> out){
            out.output(line.getValue());
        }
    }

    public static void main(String[] args) {
        Map<Integer, String> lines = new HashMap<Integer, String>();
        lines.put(1, "To be, or not to be: that is the question: ");
        lines.put(2, "Whether 'tis nobler in the mind to suffer ");
        lines.put(3, "The slings and arrows of outrageous fortune, ");
        lines.put(4, "Or to take arms against a sea of troubles, ");

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);


        PCollection<KV<Integer, String>> input = pipeline.apply(Create.of(lines)).setCoder(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()));
        String outputPath = new File("src/main/resources/beam_output/").getPath();

        input.apply("GetValueOnly", ParDo.of(new doNothing()))
            .apply(TextIO.write().to(outputPath + "/output-"));
        pipeline.run().waitUntilFinish();
    }
}
