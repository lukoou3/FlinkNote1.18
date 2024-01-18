package com.java.flink.stream.high.config;

import com.java.flink.base.FlinkBaseTest;
import org.junit.Test;

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/deployment/config
 * 除了jm tm内存外，一些常用配置:
 *     pipeline.object-reuse: operator chain内函数之间启用对象重用，默认false。
 *     pipeline.operator-chaining.enabled: Operator chaining优化，默认true。
 *
 *     pipeline.name: The job name used for printing and logging.默认none。
 *
 *     提交本地jar到集群
 *     pipeline.jars: A semicolon-separated list of the jars to package with the job jars to be sent to the cluster. These have to be valid paths.
 *     pipeline.classpaths: A semicolon-separated list of the classpaths to package with the job jars to be sent to the cluster. These have to be valid URLs.
 *     官方配置例子：https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/python/dependency_management/
 *       # Specify a list of jar URLs via "pipeline.jars". The jars are separated by ";"
 *       # and will be uploaded to the cluster.
 *       # NOTE: Only local file URLs (start with "file://") are supported.
 *       table_env.get_config().set("pipeline.jars", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")
 *
 *       # It looks like the following on Windows:
 *       table_env.get_config().set("pipeline.jars", "file:///E:/my/jar/path/connector.jar;file:///E:/my/jar/path/udf.jar")
 *
 *       # Specify a list of URLs via "pipeline.classpaths". The URLs are separated by ";"
 *       # and will be added to the classpath during job execution.
 *       # NOTE: The paths must specify a protocol (e.g. file://) and users should ensure that the URLs are accessible on both the client and the cluster.
 *       table_env.get_config().set("pipeline.classpaths", "file:///my/jar/path/connector.jar;file:///my/jar/path/udf.jar")
 *
 *
 *    cluster.evenly-spread-out-slots: 1.10版本开始允许在所有的 TaskManager 上均匀地分布任务。默认true。 
 */
public class ConfigTest extends FlinkBaseTest {



}
