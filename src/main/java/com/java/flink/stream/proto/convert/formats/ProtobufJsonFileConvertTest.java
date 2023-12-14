package com.java.flink.stream.proto.convert.formats;

import com.alibaba.fastjson2.JSON;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.java.flink.stream.proto.convert.types.StructType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ProtobufJsonFileConvertTest {

    public static void main(String[] args) throws Exception{
        String log = "{\"common_schema_type\":\"SSL\",\"common_sessions\":1,\"ssl_sni\":\"172.17.52.134\",\"ssl_ja3s_hash\":\"db95581d41f9b6ff07c355ef122ba684\",\"ssl_cn\":\"FTS3KET621000017\",\"ssl_cert_issuer\":\"CN=support;O=Fortinet;C=US;S=California;L=Sunnyvale;;U=Certificate Authority\",\"ssl_cert_subject\":\"CN=FTS3KET621000017;O=Fortinet;C=US;S=California;L=Sunnyvale;;U=FortiTester\",\"ssl_con_latency_ms\":3,\"common_first_ttl\":255,\"common_c2s_ipfrag_num\":0,\"common_s2c_ipfrag_num\":0,\"common_c2s_tcp_unorder_num\":0,\"common_s2c_tcp_unorder_num\":0,\"common_c2s_tcp_lostlen\":0,\"common_s2c_tcp_lostlen\":0,\"common_c2s_pkt_retrans\":0,\"common_s2c_pkt_retrans\":0,\"common_c2s_byte_retrans\":0,\"common_s2c_byte_retrans\":0,\"common_c2s_byte_diff\":962,\"common_c2s_pkt_diff\":6,\"common_s2c_byte_diff\":1532,\"common_s2c_pkt_diff\":5,\"common_direction\":69,\"common_app_full_path\":\"ssl.https\",\"common_app_label\":\"https\",\"common_app_id\":\"{\\\"LPI_L7\\\":[{\\\"app_name\\\":\\\"ssl\\\",\\\"app_id\\\":199,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":3},{\\\"app_name\\\":\\\"https\\\",\\\"app_id\\\":68,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":3}],\\\"QM_L7\\\":[{\\\"app_name\\\":\\\"ssl\\\",\\\"app_id\\\":199,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":4},{\\\"app_name\\\":\\\"https\\\",\\\"app_id\\\":68,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":4}]}\",\"common_tcp_client_isn\":2886039554,\"common_tcp_server_isn\":994057687,\"common_server_ip\":\"172.17.52.134\",\"common_client_ip\":\"172.17.51.23\",\"common_server_port\":443,\"common_client_port\":28529,\"common_stream_dir\":3,\"common_address_type\":4,\"common_start_time\":1694584704,\"common_end_time\":1694584704,\"common_con_duration_ms\":4,\"common_s2c_pkt_num\":5,\"common_s2c_byte_num\":1532,\"common_c2s_pkt_num\":6,\"common_c2s_byte_num\":962,\"common_establish_latency_ms\":1,\"ssl_ja3_hash\":\"c74588818f9d0fba88795679f0b026d1\",\"common_out_src_mac\":\"00:90:0b:e9:98:01\",\"common_out_dest_mac\":\"00:90:0b:e9:98:05\",\"common_in_src_mac\":\"00:90:0b:e9:98:05\",\"common_in_dest_mac\":\"00:90:0b:e9:98:01\",\"common_flags\":1720737928,\"common_flags_identify_info\":\"{\\\"Client is Local\\\":1,\\\"Inbound\\\":11,\\\"C2S\\\":1,\\\"S2C\\\":2,\\\"longest_run\\\":3,\\\"overlapping_template_matching\\\":3,\\\"random_excursions\\\":3,\\\"random_excursions_variant\\\":3,\\\"self_correlation\\\":3,\\\"binary_derivative\\\":3}\",\"common_protocol_label\":\"ETHERNET.IPv4.TCP\",\"common_stream_trace_id\":292763154394404071,\"common_l4_protocol\":\"IPv4_TCP\",\"common_sled_ip\":\"192.168.40.39\",\"common_device_id\":\"9800165603247024\",\"common_data_center\":\"center-xxg-tsgx\",\"common_device_tag\":\"{\\\"tags\\\":[{\\\"tag\\\":\\\"data_center\\\",\\\"value\\\":\\\"center-xxg-tsgx\\\"},{\\\"tag\\\":\\\"device_group\\\",\\\"value\\\":\\\"group-xxg-tsgx\\\"}]}\",\"common_t_vsys_id\":1,\"common_vsys_id\":1,\"common_ingestion_time\":1694584704,\"common_log_id\":21981901648766980,\"common_client_asn\":\"\",\"common_subscriber_id\":\"\",\"common_internal_ip\":\"172.17.51.23\",\"common_server_asn\":\"\",\"common_external_ip\":\"172.17.52.134\",\"common_device_group\":\"group-xxg-tsgx\",\"common_processing_time\":1694584704,\"common_recv_time\":1694584704,\"http_domain\":\"\",\"common_server_domain\":\"\",\"common_server_fqdn\":\"172.17.52.134\"}";
        Map<String, Object> map = JSON.parseObject(log);

        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(
                IOUtils.resourceToByteArray("/log_test.desc"),
                "LogTest"
        );

        ProtobufSerializer serializer = new ProtobufSerializer(descriptor);
        byte[] bytes = serializer.serialize(map);

        StructType structType = SchemaConverters.toStructType(descriptor);
        SchemaConverters.MessageConverter messageConverter = new SchemaConverters.MessageConverter(descriptor, structType);
        CodedInputStream input = CodedInputStream.newInstance(bytes);
        Map<String, Object> data = messageConverter.converter(input);

        bytes = serializer.serialize(data);
        input = CodedInputStream.newInstance(bytes);
        Map<String, Object> result = messageConverter.converter(input);
        new HashMap<String, Object>().entrySet();

        System.out.println(serializer.serialize(result).length);
        System.out.println(JSON.toJSONString(result).getBytes(StandardCharsets.UTF_8).length);
        System.out.println(JSON.toJSONBytes(result).length);

        if(args[0].equals("read")){
            if(args[1].equals("proto")){
                bytes = serializer.serialize(result);
                for (int k = 0; k < 1000000; k++) {
                    long start = System.currentTimeMillis();

                    for (int i = 0; i < 100000; i++) {
                        input = CodedInputStream.newInstance(bytes);
                        data = messageConverter.converter(input);
                    }

                    long end = System.currentTimeMillis();

                    System.out.println(args[0] +"-" + args[1] + ":" + (end - start));
                }
            }else if (args[1].equals("json")) {
                bytes = JSON.toJSONBytes(result);
                for (int k = 0; k < 1000000; k++) {
                    long start = System.currentTimeMillis();

                    for (int i = 0; i < 100000; i++) {
                        data = JSON.parseObject(bytes);
                    }

                    long end = System.currentTimeMillis();

                    System.out.println(args[0] +"-" + args[1] + ":" + (end - start));
                }
            }

        } else if (args[0].equals("write")) {
            if(args[1].equals("proto")){
                for (int k = 0; k < 1000000; k++) {
                    long start = System.currentTimeMillis();

                    for (int i = 0; i < 100000; i++) {
                        serializer.serialize(result);
                    }

                    long end = System.currentTimeMillis();

                    System.out.println(args[0] +"-" + args[1] + ":" + (end - start));
                }
            }else if (args[1].equals("json")) {
                for (int k = 0; k < 1000000; k++) {
                    long start = System.currentTimeMillis();

                    for (int i = 0; i < 100000; i++) {
                        JSON.toJSONBytes(result);
                    }

                    long end = System.currentTimeMillis();

                    System.out.println(args[0] +"-" + args[1] + ":" + (end - start));
                }
            }
        }

    }

    @Test
    public void testConvertRead() throws Exception{
        String log = "{\"common_schema_type\":\"SSL\",\"common_sessions\":1,\"ssl_sni\":\"172.17.52.134\",\"ssl_ja3s_hash\":\"db95581d41f9b6ff07c355ef122ba684\",\"ssl_cn\":\"FTS3KET621000017\",\"ssl_cert_issuer\":\"CN=support;O=Fortinet;C=US;S=California;L=Sunnyvale;;U=Certificate Authority\",\"ssl_cert_subject\":\"CN=FTS3KET621000017;O=Fortinet;C=US;S=California;L=Sunnyvale;;U=FortiTester\",\"ssl_con_latency_ms\":3,\"common_first_ttl\":255,\"common_c2s_ipfrag_num\":0,\"common_s2c_ipfrag_num\":0,\"common_c2s_tcp_unorder_num\":0,\"common_s2c_tcp_unorder_num\":0,\"common_c2s_tcp_lostlen\":0,\"common_s2c_tcp_lostlen\":0,\"common_c2s_pkt_retrans\":0,\"common_s2c_pkt_retrans\":0,\"common_c2s_byte_retrans\":0,\"common_s2c_byte_retrans\":0,\"common_c2s_byte_diff\":962,\"common_c2s_pkt_diff\":6,\"common_s2c_byte_diff\":1532,\"common_s2c_pkt_diff\":5,\"common_direction\":69,\"common_app_full_path\":\"ssl.https\",\"common_app_label\":\"https\",\"common_app_id\":\"{\\\"LPI_L7\\\":[{\\\"app_name\\\":\\\"ssl\\\",\\\"app_id\\\":199,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":3},{\\\"app_name\\\":\\\"https\\\",\\\"app_id\\\":68,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":3}],\\\"QM_L7\\\":[{\\\"app_name\\\":\\\"ssl\\\",\\\"app_id\\\":199,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":4},{\\\"app_name\\\":\\\"https\\\",\\\"app_id\\\":68,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":4}]}\",\"common_tcp_client_isn\":2886039554,\"common_tcp_server_isn\":994057687,\"common_server_ip\":\"172.17.52.134\",\"common_client_ip\":\"172.17.51.23\",\"common_server_port\":443,\"common_client_port\":28529,\"common_stream_dir\":3,\"common_address_type\":4,\"common_start_time\":1694584704,\"common_end_time\":1694584704,\"common_con_duration_ms\":4,\"common_s2c_pkt_num\":5,\"common_s2c_byte_num\":1532,\"common_c2s_pkt_num\":6,\"common_c2s_byte_num\":962,\"common_establish_latency_ms\":1,\"ssl_ja3_hash\":\"c74588818f9d0fba88795679f0b026d1\",\"common_out_src_mac\":\"00:90:0b:e9:98:01\",\"common_out_dest_mac\":\"00:90:0b:e9:98:05\",\"common_in_src_mac\":\"00:90:0b:e9:98:05\",\"common_in_dest_mac\":\"00:90:0b:e9:98:01\",\"common_flags\":1720737928,\"common_flags_identify_info\":\"{\\\"Client is Local\\\":1,\\\"Inbound\\\":11,\\\"C2S\\\":1,\\\"S2C\\\":2,\\\"longest_run\\\":3,\\\"overlapping_template_matching\\\":3,\\\"random_excursions\\\":3,\\\"random_excursions_variant\\\":3,\\\"self_correlation\\\":3,\\\"binary_derivative\\\":3}\",\"common_protocol_label\":\"ETHERNET.IPv4.TCP\",\"common_stream_trace_id\":292763154394404071,\"common_l4_protocol\":\"IPv4_TCP\",\"common_sled_ip\":\"192.168.40.39\",\"common_device_id\":\"9800165603247024\",\"common_data_center\":\"center-xxg-tsgx\",\"common_device_tag\":\"{\\\"tags\\\":[{\\\"tag\\\":\\\"data_center\\\",\\\"value\\\":\\\"center-xxg-tsgx\\\"},{\\\"tag\\\":\\\"device_group\\\",\\\"value\\\":\\\"group-xxg-tsgx\\\"}]}\",\"common_t_vsys_id\":1,\"common_vsys_id\":1,\"common_ingestion_time\":1694584704,\"common_log_id\":21981901648766980,\"common_client_asn\":\"\",\"common_subscriber_id\":\"\",\"common_internal_ip\":\"172.17.51.23\",\"common_server_asn\":\"\",\"common_external_ip\":\"172.17.52.134\",\"common_device_group\":\"group-xxg-tsgx\",\"common_processing_time\":1694584704,\"common_recv_time\":1694584704,\"http_domain\":\"\",\"common_server_domain\":\"\",\"common_server_fqdn\":\"172.17.52.134\"}";
        Map<String, Object> map = JSON.parseObject(log);

        String path = getClass().getResource("/protobuf/log_test.desc").getPath();
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(
                ProtobufUtils.readDescriptorFileContent(path),
                "LogTest"
        );

        ProtobufSerializer serializer = new ProtobufSerializer(descriptor);
        byte[] bytes = serializer.serialize(map);

        StructType structType = SchemaConverters.toStructType(descriptor);
        SchemaConverters.MessageConverter messageConverter = new SchemaConverters.MessageConverter(descriptor, structType);
        CodedInputStream input = CodedInputStream.newInstance(bytes);
        Map<String, Object> data = messageConverter.converter(input);

        bytes = serializer.serialize(data);
        input = CodedInputStream.newInstance(bytes);
        Map<String, Object> result = messageConverter.converter(input);

        System.out.println(bytes.length);
        System.out.println(JSON.toJSONString(data).getBytes(StandardCharsets.UTF_8).length);

        System.out.println(JSON.toJSONString(data));
        System.out.println(JSON.toJSONString(result));

        byte[] jsonBytes = JSON.toJSONString(data).getBytes(StandardCharsets.UTF_8);
        byte[] protoBytes = bytes;

        for (int k = 0; k < 100; k++) {
            long start = System.currentTimeMillis();

            for (int i = 0; i < 10000; i++) {
                input = CodedInputStream.newInstance(protoBytes);
                data = messageConverter.converter(input);
            }

            long end = System.currentTimeMillis();

            System.out.println(end - start);
        }

        System.out.println(StringUtils.repeat('#', 100));


        for (int k = 0; k < 100; k++) {
            long start = System.currentTimeMillis();

            for (int i = 0; i < 10000; i++) {
                //data = JSON.parseObject(jsonBytes);
                data = JSON.parseObject(new String(jsonBytes, StandardCharsets.UTF_8));
            }

            long end = System.currentTimeMillis();

            System.out.println(end - start);
        }



    }


    @Test
    public void testConvertWrite() throws Exception{
        String log = "{\"common_schema_type\":\"SSL\",\"common_sessions\":1,\"ssl_sni\":\"172.17.52.134\",\"ssl_ja3s_hash\":\"db95581d41f9b6ff07c355ef122ba684\",\"ssl_cn\":\"FTS3KET621000017\",\"ssl_cert_issuer\":\"CN=support;O=Fortinet;C=US;S=California;L=Sunnyvale;;U=Certificate Authority\",\"ssl_cert_subject\":\"CN=FTS3KET621000017;O=Fortinet;C=US;S=California;L=Sunnyvale;;U=FortiTester\",\"ssl_con_latency_ms\":3,\"common_first_ttl\":255,\"common_c2s_ipfrag_num\":0,\"common_s2c_ipfrag_num\":0,\"common_c2s_tcp_unorder_num\":0,\"common_s2c_tcp_unorder_num\":0,\"common_c2s_tcp_lostlen\":0,\"common_s2c_tcp_lostlen\":0,\"common_c2s_pkt_retrans\":0,\"common_s2c_pkt_retrans\":0,\"common_c2s_byte_retrans\":0,\"common_s2c_byte_retrans\":0,\"common_c2s_byte_diff\":962,\"common_c2s_pkt_diff\":6,\"common_s2c_byte_diff\":1532,\"common_s2c_pkt_diff\":5,\"common_direction\":69,\"common_app_full_path\":\"ssl.https\",\"common_app_label\":\"https\",\"common_app_id\":\"{\\\"LPI_L7\\\":[{\\\"app_name\\\":\\\"ssl\\\",\\\"app_id\\\":199,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":3},{\\\"app_name\\\":\\\"https\\\",\\\"app_id\\\":68,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":3}],\\\"QM_L7\\\":[{\\\"app_name\\\":\\\"ssl\\\",\\\"app_id\\\":199,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":4},{\\\"app_name\\\":\\\"https\\\",\\\"app_id\\\":68,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":4}]}\",\"common_tcp_client_isn\":2886039554,\"common_tcp_server_isn\":994057687,\"common_server_ip\":\"172.17.52.134\",\"common_client_ip\":\"172.17.51.23\",\"common_server_port\":443,\"common_client_port\":28529,\"common_stream_dir\":3,\"common_address_type\":4,\"common_start_time\":1694584704,\"common_end_time\":1694584704,\"common_con_duration_ms\":4,\"common_s2c_pkt_num\":5,\"common_s2c_byte_num\":1532,\"common_c2s_pkt_num\":6,\"common_c2s_byte_num\":962,\"common_establish_latency_ms\":1,\"ssl_ja3_hash\":\"c74588818f9d0fba88795679f0b026d1\",\"common_out_src_mac\":\"00:90:0b:e9:98:01\",\"common_out_dest_mac\":\"00:90:0b:e9:98:05\",\"common_in_src_mac\":\"00:90:0b:e9:98:05\",\"common_in_dest_mac\":\"00:90:0b:e9:98:01\",\"common_flags\":1720737928,\"common_flags_identify_info\":\"{\\\"Client is Local\\\":1,\\\"Inbound\\\":11,\\\"C2S\\\":1,\\\"S2C\\\":2,\\\"longest_run\\\":3,\\\"overlapping_template_matching\\\":3,\\\"random_excursions\\\":3,\\\"random_excursions_variant\\\":3,\\\"self_correlation\\\":3,\\\"binary_derivative\\\":3}\",\"common_protocol_label\":\"ETHERNET.IPv4.TCP\",\"common_stream_trace_id\":292763154394404071,\"common_l4_protocol\":\"IPv4_TCP\",\"common_sled_ip\":\"192.168.40.39\",\"common_device_id\":\"9800165603247024\",\"common_data_center\":\"center-xxg-tsgx\",\"common_device_tag\":\"{\\\"tags\\\":[{\\\"tag\\\":\\\"data_center\\\",\\\"value\\\":\\\"center-xxg-tsgx\\\"},{\\\"tag\\\":\\\"device_group\\\",\\\"value\\\":\\\"group-xxg-tsgx\\\"}]}\",\"common_t_vsys_id\":1,\"common_vsys_id\":1,\"common_ingestion_time\":1694584704,\"common_log_id\":21981901648766980,\"common_client_asn\":\"\",\"common_subscriber_id\":\"\",\"common_internal_ip\":\"172.17.51.23\",\"common_server_asn\":\"\",\"common_external_ip\":\"172.17.52.134\",\"common_device_group\":\"group-xxg-tsgx\",\"common_processing_time\":1694584704,\"common_recv_time\":1694584704,\"http_domain\":\"\",\"common_server_domain\":\"\",\"common_server_fqdn\":\"172.17.52.134\"}";
        Map<String, Object> map = JSON.parseObject(log);

        String path = getClass().getResource("/protobuf/log_test.desc").getPath();
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(
                ProtobufUtils.readDescriptorFileContent(path),
                "LogTest"
        );

        ProtobufSerializer serializer = new ProtobufSerializer(descriptor);
        byte[] bytes = serializer.serialize(map);

        StructType structType = SchemaConverters.toStructType(descriptor);
        SchemaConverters.MessageConverter messageConverter = new SchemaConverters.MessageConverter(descriptor, structType);
        CodedInputStream input = CodedInputStream.newInstance(bytes);
        Map<String, Object> data = messageConverter.converter(input);

        bytes = serializer.serialize(data);
        input = CodedInputStream.newInstance(bytes);
        Map<String, Object> result = messageConverter.converter(input);

        System.out.println(serializer.serialize(result).length);
        System.out.println(JSON.toJSONString(result).getBytes(StandardCharsets.UTF_8).length);
        System.out.println(JSON.toJSONBytes(result).length);

        System.out.println(JSON.toJSONString(data));
        System.out.println(JSON.toJSONString(result));


        for (int k = 0; k < 100; k++) {
            long start = System.currentTimeMillis();

            for (int i = 0; i < 10000; i++) {
                serializer.serialize(result);
            }

            long end = System.currentTimeMillis();

            System.out.println(end - start);
        }

        System.out.println(StringUtils.repeat('#', 100));


        for (int k = 0; k < 100; k++) {
            long start = System.currentTimeMillis();

            for (int i = 0; i < 10000; i++) {
                //JSON.toJSONString(result).getBytes(StandardCharsets.UTF_8);
                JSON.toJSONBytes(result);
            }

            long end = System.currentTimeMillis();

            System.out.println(end - start);
        }

    }



}
