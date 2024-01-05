package com.java.flink.formats.protobuf;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class ProtobufOptions {
    public static final ConfigOption<String> MESSAGE_NAME =
            ConfigOptions.key("message.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription( "The protobuf MessageName to look for in descriptor file.");

    public static final ConfigOption<String> DESC_FILE_PATH =
            ConfigOptions.key("descriptor.file.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription( "The Protobuf descriptor file.");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore.parse.errors")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors, true by default.");

    public static final ConfigOption<Boolean> EMIT_DEFAULT_VALUES =
            ConfigOptions.key("emit.default.values")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag for whether to render fields with zero values when deserializing Protobuf to struct.\n"
                                    + "When a field is empty in the serialized Protobuf, this library will deserialize them as null by default. However, this flag can control whether to render the type-specific zero value.\n"
                                    + "This operates similarly to includingDefaultValues in protobuf-java-util's JsonFormat, or emitDefaults in golang/protobuf's jsonpb.");
}
