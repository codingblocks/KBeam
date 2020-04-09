package net.codingblocks

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.values.TypeDescriptors

val specialCharacterRegex = Regex("[^A-Za-z0-9]")

fun main(args: Array<String>) {
    val p = Pipeline.create()

    p
        .apply<PCollection<String>>(
            "Reading files",
            TextIO.read().from("*")
        )
        .apply(
            "Flat mapping",
            FlatMapElements
                .into(TypeDescriptors.strings())
                .via(
                    ProcessFunction {
                        it.split("")
                    }
                )
        )
        .apply(
            "Filter special characters out",
            Filter
                .by(
                    SerializableFunction<String, Boolean> {
                        it.isNotEmpty() && !it.matches(specialCharacterRegex)
                    }
                )
        )
        .apply(
            "Convert to uppercase",
            ParDo.of(
                object : DoFn<String, String>() {
                    @ProcessElement
                    fun processElement(context: ProcessContext) {
                        context.output(context.element().toUpperCase())
                    }
                }
            )
        )
        .apply(
            "Count letters",
            Count.perElement<String>()
        )
        .apply(
            "Format output",
            MapElements
                .into(TypeDescriptors.strings())
                .via(ProcessFunction {
                    "${it.key} : ${it.value}"
                })
        )
        .apply(
            "Write to output files",
            TextIO.write().to("output")
        )

    // TODO next?
    // Streaming data in from Kafka
    // Writing unit tests / refactoring
    // Change the use case to something cooler
    // Get this running in Dataflow

    p.run()
}
