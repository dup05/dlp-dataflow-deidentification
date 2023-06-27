package com.google.swarm.tokenization.common;

import com.google.auto.value.AutoValue;
import com.google.swarm.tokenization.avro.GenericRecordCoder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@AutoValue
@SuppressWarnings("serial")
public abstract class WriteToCSVObject extends PTransform<PCollection<KV<String, String>>, PDone> {

    public abstract String outputBucket();

    public static Builder newBuilder() {
        return new AutoValue_WriteToCSVObject.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        public abstract Builder setOutputBucket(String value);

        public abstract WriteToCSVObject build();
    }

    @Override
    public PDone expand(PCollection<KV<String, String>> input) {
        return input.apply("WriteIO", TextIO.<KV<String, String>>writeCustomType()
                .to(new GCSDestination("gs://temp"))
                .withSuffix(".csv"));
//        TextIO.writeCustomType[Event].asInstanceOf[TextIO.TypedWrite[Event, String]]
//    .to(new MyDynamicDestinations(baseDir))
//                TextIO.<KV<String,String>>writeCustomType()
//                        .to(new GCSDestination(outputBucket())));

    }

    public class GCSDestination extends DynamicDestinations< KV<String,String>, String, String> {

        public String outputBucket;

        public GCSDestination(String outputBucket) {
            this.outputBucket = outputBucket;
        }


        @Override
        public String formatRecord(KV<String, String> record) {
            return record.getValue();
        }

        @Override
        public String getDestination(KV<String, String> element) {
            String filename = element.getKey().split("/")[-1].split(".")[0];
            return outputBucket + "/" + filename + ".csv";
        }

        @Override
        public String getDefaultDestination() {
            return outputBucket + "/result.csv";
        }

        @Override
        public FileBasedSink.@UnknownKeyFor @NonNull @Initialized FilenamePolicy getFilenamePolicy(String destination) {
            return null;
        }
    }
//    public class GCSDestination implements SerializableFunction<KV<String,String>, DefaultFilenamePolicy.Params> {
//        public String outputBucket;
//
//        public GCSDestination(String outputBucket) {
//            this.outputBucket = outputBucket;
//        }
//
//    @Override
//    public DefaultFilenamePolicy.Params apply(KV<String, String> input) {
//            String filename = input.getKey().split("/")[-1].split(".")[0];
//            return new DefaultFilenamePolicy.Params()
//                .withBaseFilename(outputBucket + "/" + filename + ".csv")
//                        );
//    }

//        @Override
//        public DefaultFilenamePolicy.Params apply(UserWriteType input) {
//            return new DefaultFilenamePolicy.Params()
//                    .withBaseFilename(
//                            baseDir.resolve(
//                                    "file_" + input.destination.substring(0, 1) + ".txt",
//                                    ResolveOptions.StandardResolveOptions.RESOLVE_FILE));
//        }
//    }

}