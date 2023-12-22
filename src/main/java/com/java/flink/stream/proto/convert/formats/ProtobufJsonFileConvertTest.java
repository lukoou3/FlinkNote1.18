package com.java.flink.stream.proto.convert.formats;

import com.alibaba.fastjson2.JSON;
import com.google.common.primitives.Bytes;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.java.flink.stream.proto.convert.types.StructType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
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
            }else if(args[1].equals("proto2")){
                bytes = serializer.serialize(result);
                for (int k = 0; k < 1000000; k++) {
                    long start = System.currentTimeMillis();

                    for (int i = 0; i < 100000; i++) {
                        input = CodedInputStream.newInstance(bytes);
                        data = messageConverter.converter2(input);
                    }

                    long end = System.currentTimeMillis();

                    System.out.println(args[0] +"-" + args[1] + ":" + (end - start));
                }
            }else if(args[1].equals("proto3")){
                bytes = serializer.serialize(result);
                for (int k = 0; k < 1000000; k++) {
                    long start = System.currentTimeMillis();

                    for (int i = 0; i < 100000; i++) {
                        input = CodedInputStream.newInstance(bytes);
                        data = messageConverter.converter3(input);
                    }

                    long end = System.currentTimeMillis();

                    System.out.println(args[0] +"-" + args[1] + ":" + (end - start));
                }
            }
            else if (args[1].equals("protoEmitDefaultValues")) {
                bytes = serializer.serialize(result);
                SchemaConverters.MessageConverter messageConverter2 = new SchemaConverters.MessageConverter(descriptor, structType, true);
                for (int k = 0; k < 1000000; k++) {
                    long start = System.currentTimeMillis();

                    for (int i = 0; i < 100000; i++) {
                        input = CodedInputStream.newInstance(bytes);
                        data = messageConverter2.converter(input);
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
    public void test() throws Exception {
        String str = "{\"tcp_rtt_ms\":127,\"http_version\":\"http1\",\"http_request_line\":\"GET /announce?info_hash=%C3%9F%85OV%A8%5B%14E%D8T%F9%83%C3.%1FA2%5E%9F&peer_id=-XL0012-%0B%FC%B8%B0%F8%AA%B5%03%8EEH%B2&port=15000&uploaded=0&downloaded=0&left=2908687231&numwant=200&key=23243&compact=1&event=started HTTP/1.0\",\"http_host\":\"torrentsmd.com\",\"http_url\":\"torrentsmd.com/announce?info_hash=ß OV [_E T   ._A2^ &peer_id=-XL0012-_      _ EH &port=15000&uploaded=0&downloaded=0&left=2908687231&numwant=200&key=23243&compact=1&event=started\",\"http_user_agent\":\"uTorrent\",\"http_status_code\":403,\"http_response_line\":\"HTTP/1.1 403 Forbidden\",\"http_response_content_type\":\"text/html; charset=UTF-8\",\"http_response_content_length\":4518,\"http_response_latency_ms\":56,\"http_session_duration_ms\":56,\"in_src_mac\":\"58:b3:8f:fa:3b:11\",\"in_dest_mac\":\"48:73:97:96:38:27\",\"out_src_mac\":\"48:73:97:96:38:27\",\"out_dest_mac\":\"58:b3:8f:fa:3b:11\",\"tcp_client_isn\":3970156621,\"tcp_server_isn\":4201369837,\"address_type\":4,\"client_ip\":\"192.168.64.33\",\"server_ip\":\"172.67.140.59\",\"client_port\":64506,\"server_port\":8080,\"in_link_id\":65535,\"out_link_id\":65535,\"start_timestamp_ms\":1703227522672,\"end_timestamp_ms\":1703227522832,\"duration_ms\":160,\"sent_pkts\":6,\"sent_bytes\":665,\"received_pkts\":8,\"received_bytes\":5627,\"tcp_c2s_ip_fragments\":0,\"tcp_s2c_ip_fragments\":0,\"tcp_c2s_rtx_pkts\":0,\"tcp_c2s_rtx_bytes\":0,\"tcp_s2c_rtx_pkts\":0,\"tcp_s2c_rtx_bytes\":0,\"tcp_c2s_o3_pkts\":0,\"tcp_s2c_o3_pkts\":0,\"tcp_c2s_lost_bytes\":0,\"tcp_s2c_lost_bytes\":0,\"flags\":24584,\"flags_identify_info\":[1,1,3],\"app_transition\":\"http.1111.cloudflare\",\"server_geolocation\":\"美国.Unknown.Unknown..\",\"decoded_as\":\"HTTP\",\"server_fqdn\":\"torrentsmd.com\",\"app\":\"1111\",\"decoded_path\":\"ETHERNET.IPv4.TCP.http\",\"fqdn_category_list\":[2832,2800,30,31],\"t_vsys_id\":1,\"vsys_id\":1,\"session_id\":2.9059959871878e+17,\"tcp_handshake_latency_ms\":52,\"client_os_desc\":\"Windows\",\"server_os_desc\":\"Windows\",\"data_center\":\"center-xxg-tsgx\",\"device_group\":\"group-xxg-tsgx\",\"device_tag\":\"{\\\"tags\\\":[{\\\"tag\\\":\\\"data_center\\\",\\\"value\\\":\\\"center-xxg-tsgx\\\"},{\\\"tag\\\":\\\"device_group\\\",\\\"value\\\":\\\"group-xxg-tsgx\\\"}]}\",\"device_id\":\"9800165603247024\",\"sled_ip\":\"192.168.40.39\",\"dup_traffic_flag\":0,\"shaping_rule_list\":[369704]}";
        JSON.parseObject(str.getBytes());
    }

    @Test
    public void testJsonParse() throws Exception {
        String log = "{\"common_schema_type\":\"SSL\",\"common_sessions\":1,\"ssl_sni\":\"172.17.52.134\",\"ssl_ja3s_hash\":\"db95581d41f9b6ff07c355ef122ba684\",\"ssl_cn\":\"FTS3KET621000017\",\"ssl_cert_issuer\":\"CN=support;O=Fortinet;C=US;S=California;L=Sunnyvale;;U=Certificate Authority\",\"ssl_cert_subject\":\"CN=FTS3KET621000017;O=Fortinet;C=US;S=California;L=Sunnyvale;;U=FortiTester\",\"ssl_con_latency_ms\":3,\"common_first_ttl\":255,\"common_c2s_ipfrag_num\":0,\"common_s2c_ipfrag_num\":0,\"common_c2s_tcp_unorder_num\":0,\"common_s2c_tcp_unorder_num\":0,\"common_c2s_tcp_lostlen\":0,\"common_s2c_tcp_lostlen\":0,\"common_c2s_pkt_retrans\":0,\"common_s2c_pkt_retrans\":0,\"common_c2s_byte_retrans\":0,\"common_s2c_byte_retrans\":0,\"common_c2s_byte_diff\":962,\"common_c2s_pkt_diff\":6,\"common_s2c_byte_diff\":1532,\"common_s2c_pkt_diff\":5,\"common_direction\":69,\"common_app_full_path\":\"ssl.https\",\"common_app_label\":\"https\",\"common_app_id\":\"{\\\"LPI_L7\\\":[{\\\"app_name\\\":\\\"ssl\\\",\\\"app_id\\\":199,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":3},{\\\"app_name\\\":\\\"https\\\",\\\"app_id\\\":68,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":3}],\\\"QM_L7\\\":[{\\\"app_name\\\":\\\"ssl\\\",\\\"app_id\\\":199,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":4},{\\\"app_name\\\":\\\"https\\\",\\\"app_id\\\":68,\\\"surrogate_id\\\":0,\\\"packet_sequence\\\":4}]}\",\"common_tcp_client_isn\":2886039554,\"common_tcp_server_isn\":994057687,\"common_server_ip\":\"172.17.52.134\",\"common_client_ip\":\"172.17.51.23\",\"common_server_port\":443,\"common_client_port\":28529,\"common_stream_dir\":3,\"common_address_type\":4,\"common_start_time\":1694584704,\"common_end_time\":1694584704,\"common_con_duration_ms\":4,\"common_s2c_pkt_num\":5,\"common_s2c_byte_num\":1532,\"common_c2s_pkt_num\":6,\"common_c2s_byte_num\":962,\"common_establish_latency_ms\":1,\"ssl_ja3_hash\":\"c74588818f9d0fba88795679f0b026d1\",\"common_out_src_mac\":\"00:90:0b:e9:98:01\",\"common_out_dest_mac\":\"00:90:0b:e9:98:05\",\"common_in_src_mac\":\"00:90:0b:e9:98:05\",\"common_in_dest_mac\":\"00:90:0b:e9:98:01\",\"common_flags\":1720737928,\"common_flags_identify_info\":\"{\\\"Client is Local\\\":1,\\\"Inbound\\\":11,\\\"C2S\\\":1,\\\"S2C\\\":2,\\\"longest_run\\\":3,\\\"overlapping_template_matching\\\":3,\\\"random_excursions\\\":3,\\\"random_excursions_variant\\\":3,\\\"self_correlation\\\":3,\\\"binary_derivative\\\":3}\",\"common_protocol_label\":\"ETHERNET.IPv4.TCP\",\"common_stream_trace_id\":292763154394404071,\"common_l4_protocol\":\"IPv4_TCP\",\"common_sled_ip\":\"192.168.40.39\",\"common_device_id\":\"9800165603247024\",\"common_data_center\":\"center-xxg-tsgx\",\"common_device_tag\":\"{\\\"tags\\\":[{\\\"tag\\\":\\\"data_center\\\",\\\"value\\\":\\\"center-xxg-tsgx\\\"},{\\\"tag\\\":\\\"device_group\\\",\\\"value\\\":\\\"group-xxg-tsgx\\\"}]}\",\"common_t_vsys_id\":1,\"common_vsys_id\":1,\"common_ingestion_time\":1694584704,\"common_log_id\":21981901648766980,\"common_client_asn\":\"\",\"common_subscriber_id\":\"\",\"common_internal_ip\":\"172.17.51.23\",\"common_server_asn\":\"\",\"common_external_ip\":\"172.17.52.134\",\"common_device_group\":\"group-xxg-tsgx\",\"common_processing_time\":1694584704,\"common_recv_time\":1694584704,\"http_domain\":\"\",\"common_server_domain\":\"\",\"common_server_fqdn\":\"172.17.52.134\"}";
        byte[] bytes = log.getBytes(StandardCharsets.UTF_8);

        for (int k = 0; k < 100; k++) {
            long start = System.currentTimeMillis();

            for (int i = 0; i < 10000; i++) {
                log = new String(bytes, StandardCharsets.UTF_8);
                Map<String, Object> map = JSON.parseObject(log);
            }

            long end = System.currentTimeMillis();

            System.out.println(end - start);
        }

        System.out.println(StringUtils.repeat('#', 100));

        for (int k = 0; k < 100; k++) {
            long start = System.currentTimeMillis();

            for (int i = 0; i < 10000; i++) {
                Map<String, Object> map = JSON.parseObject(bytes);
            }

            long end = System.currentTimeMillis();

            System.out.println(end - start);
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

        SchemaConverters.MessageConverter messageConverter2 = new SchemaConverters.MessageConverter(descriptor, structType, true);
        CodedInputStream input2 = CodedInputStream.newInstance(bytes);
        Map<String, Object> data2 = messageConverter2.converter(input2);
        System.out.println(JSON.toJSONString(data2));

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
                input2 = CodedInputStream.newInstance(protoBytes);
                data = messageConverter2.converter(input2);
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

    @Test
    public void testSizeAndCompressSize() throws Exception{
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

        // proto size
        System.out.println(serializer.serialize(result).length);
        // json size
        System.out.println(JSON.toJSONString(result).getBytes(StandardCharsets.UTF_8).length);
        System.out.println(JSON.toJSONBytes(result).length);

        System.out.println(JSON.toJSONString(data));
        System.out.println(JSON.toJSONString(result));

        System.out.println(StringUtils.repeat('#', 50));

        CompressionType[] compressionTypes = new CompressionType[]{
                CompressionType.NONE,  CompressionType.SNAPPY,   CompressionType.LZ4, CompressionType.GZIP, CompressionType.ZSTD
        };

        for (int i = 0; i < compressionTypes.length; i++) {
            byte[] protoBytes = serializer.serialize(result);
            byte[] jsonBytes = JSON.toJSONString(result).getBytes(StandardCharsets.UTF_8);
            System.out.println(String.format("proto size:%d, json size:%d", protoBytes.length, jsonBytes.length));

            CompressionType compressionType = compressionTypes[i];
            ByteBufferOutputStream bufferStream = new ByteBufferOutputStream(1024 * 16);
            OutputStream outputStream = compressionType.wrapForOutput(bufferStream, (byte) 2);
            outputStream.write(protoBytes);
            outputStream.close();
            int protoCompressSize = bufferStream.position();

            bufferStream = new ByteBufferOutputStream(1024 * 16);
            outputStream = compressionType.wrapForOutput(bufferStream, (byte) 2);
            outputStream.write(jsonBytes);
            outputStream.close();
            int jsonCompressSize = bufferStream.position();

            System.out.println(String.format("compression(%s): proto size:%d, json size:%d", compressionType , protoCompressSize, jsonCompressSize));

            System.out.println(StringUtils.repeat('#', 50));
        }
    }

    @Test
    public void testOutJsonProtoFile() throws Exception{
        String path = getClass().getResource("/protobuf/log_test.desc").getPath();
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(
                ProtobufUtils.readDescriptorFileContent(path),
                "LogTest"
        );

        ProtobufSerializer serializer = new ProtobufSerializer(descriptor);
        StructType structType = SchemaConverters.toStructType(descriptor);
        SchemaConverters.MessageConverter messageConverter = new SchemaConverters.MessageConverter(descriptor, structType);

        FileInputStream fileInputStream = new FileInputStream("D:\\doc\\datas\\session-record-completed.data");
        LineIterator lineIterator = IOUtils.lineIterator(fileInputStream, StandardCharsets.UTF_8);
        FileOutputStream osProto = new FileOutputStream("D:\\doc\\datas\\log_test.proto");
        FileOutputStream osJson = new FileOutputStream("D:\\doc\\datas\\log_test.json");
        final byte[] writeBuffer = new byte[4];
        while (lineIterator.hasNext()){
            String line = lineIterator.next();
            Map<String, Object> map = JSON.parseObject(line);
            byte[] bytes = serializer.serialize(map);
            CodedInputStream input = CodedInputStream.newInstance(bytes);
            Map<String, Object> data = messageConverter.converter(input);

            bytes = serializer.serialize(data);
            int len = bytes.length;
            writeBuffer[3] = (byte) ((len >> 0)  & 0xFF);
            writeBuffer[2] = (byte) ((len >> 8)  & 0xFF);
            writeBuffer[1] = (byte) ((len >> 16) & 0xFF);
            writeBuffer[0] = (byte) ((len >> 24) & 0xFF);
            osProto.write(writeBuffer);
            osProto.write(bytes);

            bytes = JSON.toJSONBytes(data);
            osJson.write(bytes);
            osJson.write('\n');
        }

        osProto.close();
        osJson.close();
    }

}
