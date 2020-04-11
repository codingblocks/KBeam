package net.codingblocks

import org.apache.beam.sdk.io.kafka.KafkaRecord
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.Values
import org.apache.beam.sdk.values.PCollection

class KafkaTransform : PTransform<PCollection<KafkaRecord<String,String>>, PCollection<String>>() {
    override fun expand(input: PCollection<KafkaRecord<String, String>>?): PCollection<String> {
        return input!!.apply("Extract message",
            ParDo.of(
                object : DoFn<KafkaRecord<String,String>, String>() {
                    @ProcessElement
                    fun processElement(context: ProcessContext) {
                        println(context.element().kv.value)
                        context.output(context.element().kv.value)
                    }
                }
            )
        )
    }
}
