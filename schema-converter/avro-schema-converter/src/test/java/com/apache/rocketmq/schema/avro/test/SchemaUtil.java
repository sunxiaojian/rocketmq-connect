package com.apache.rocketmq.schema.avro.test;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class SchemaUtil {
    public static void main(String[] args) {
        Schema schema = SchemaBuilder
                .record("HandshakeRequest")
                .namespace("org.apache.avro.ipc")
                .fields()
                .name("clientHash").type().fixed("MD5").size(16).noDefault()
                .name("clientProtocol").type().nullable().stringType().noDefault()
                .name("serverHash").type("MD5").noDefault()
                .name("meta").type().nullable().map().values().bytesType().noDefault()
                .endRecord();


        Schema.Parser parser = new Schema.Parser();
        parser.parse(schema.toString());

        System.out.println(schema.toString());
    }
}
