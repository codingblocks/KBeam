package net.codingblocks

import avro.shaded.com.google.common.collect.ImmutableMap
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.options.ValueProvider
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.values.TypeDescriptors
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.reflect.typeOf

val specialCharacterRegex = Regex("[^A-Za-z0-9]")

fun main(args: Array<String>) {


    val p = Pipeline.create()

    p
        .apply(
            "Reading from Kafka",
            KafkaIO.read<String, String>()
                .withBootstrapServers("localhost:9092")
                .withTopic("raw-sightings")  // use withTopics(List<String>) to read from multiple topics.
                .withKeyDeserializer(StringDeserializer::class.java)
                .withValueDeserializer(StringDeserializer::class.java)
                // Change to include history (setting the autoResetOffset to "earliest", ensuring that offsets are committed
                .updateConsumerProperties(mutableMapOf<String,Any>("group.id" to "kbeam"))
        )
        .apply("Extract Kafka Message", KafkaTransform())

    // TODO next time
    // Pull in "Tabular" data from AlienTypes
    // "Enrich" our sightings data with the tabular data
    // Output the enriched data to another topic
    // (already setup to display enriched data on a map)
    // Writing unit tests / refactoring
    // Get this running in Dataflow

    p.run().waitUntilFinish()
}
