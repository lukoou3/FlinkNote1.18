package com.java.flink.stream.proto.convert.formats;

import com.google.protobuf.AnyProto;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.ProtocolStringList;
import org.apache.commons.io.FileUtils;
import org.apache.flink.util.Preconditions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProtobufUtils {

    public static boolean isProto3(Descriptor descriptor){
        return descriptor.getFile().getSyntax() == FileDescriptor.Syntax.PROTO3;
    }

    public static boolean isMessageSetWireFormat(Descriptor descriptor){
        return descriptor.getOptions().getMessageSetWireFormat();
    }

    public static void checkSupportParseDescriptor(Descriptor descriptor){
        Preconditions.checkArgument(isProto3(descriptor), "only support proto3");
        Preconditions.checkArgument(!isMessageSetWireFormat(descriptor), "not support message_set_wire_format option");
    }

    public static byte[] readDescriptorFileContent(String filePath)  {
        try {
            return FileUtils.readFileToByteArray(new File(filePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Descriptor buildDescriptor(byte[] bytes, String messageName)  throws Exception{
        List<FileDescriptor> fileDescriptorList = parseFileDescriptorSet(bytes);
        Descriptor descriptor = fileDescriptorList.stream().flatMap(fileDesc -> {
            return fileDesc.getMessageTypes().stream().filter(desc -> desc.getName().equals(messageName) || desc.getFullName().equals(messageName));
        }).findFirst().get();
        return descriptor;
    }

    private static List<FileDescriptor> parseFileDescriptorSet(byte[] bytes) throws Exception{
        DescriptorProtos.FileDescriptorSet fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(bytes);
        Map<String, DescriptorProtos.FileDescriptorProto> fileDescriptorProtoMap = fileDescriptorSet.getFileList().stream().collect(Collectors.toMap(x -> x.getName(), x -> x));
        List<FileDescriptor> fileDescriptorList = new ArrayList<>();
        for (DescriptorProtos.FileDescriptorProto fileDescriptorProto : fileDescriptorSet.getFileList()) {
            FileDescriptor fileDescriptor = buildFileDescriptor(fileDescriptorProto, fileDescriptorProtoMap);
            fileDescriptorList.add(fileDescriptor);
        }
        return fileDescriptorList;
    }

    private static FileDescriptor buildFileDescriptor(DescriptorProtos.FileDescriptorProto fileDescriptorProto, Map<String, DescriptorProtos.FileDescriptorProto> fileDescriptorProtoMap) throws Exception{
        ProtocolStringList dependencyList = fileDescriptorProto.getDependencyList();
        FileDescriptor[] fileDescriptorArray = new FileDescriptor[dependencyList.size()];
        for (int i = 0; i < fileDescriptorArray.length; i++) {
            String dependency = dependencyList.get(i);
            DescriptorProtos.FileDescriptorProto dependencyProto = fileDescriptorProtoMap.get(dependency);
            if (dependencyProto == null) {
                throw new IllegalArgumentException("dependency:" + dependency + "not exist");
            }
            if (dependencyProto.getName().equals("google/protobuf/any.proto")
                    && dependencyProto.getPackage().equals("google.protobuf")) {
                // For Any, use the descriptor already included as part of the Java dependency.
                // Without this, JsonFormat used for converting Any fields fails when
                // an Any field in input is set to `Any.getDefaultInstance()`.
                fileDescriptorArray[i] = AnyProto.getDescriptor();
            } else {
                fileDescriptorArray[i] = buildFileDescriptor(dependencyProto, fileDescriptorProtoMap);
            }
        }

        return FileDescriptor.buildFrom(fileDescriptorProto, fileDescriptorArray);
    }
}


