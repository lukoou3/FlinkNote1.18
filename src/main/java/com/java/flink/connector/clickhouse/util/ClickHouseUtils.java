package com.java.flink.connector.clickhouse.util;

import com.github.housepower.buffer.ByteArrayWriter;
import com.github.housepower.data.*;
import com.github.housepower.data.type.*;
import com.github.housepower.data.type.complex.DataTypeArray;
import com.github.housepower.data.type.complex.DataTypeDecimal;
import com.github.housepower.data.type.complex.DataTypeFixedString;
import com.github.housepower.data.type.complex.DataTypeString;
import com.github.housepower.jdbc.ClickHouseArray;
import com.github.housepower.jdbc.ClickHouseConnection;
import com.github.housepower.misc.BytesCharSeq;
import com.github.housepower.misc.DateTimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ClickHouseUtils {
    static final Logger LOG = LoggerFactory.getLogger(ClickHouseUtils.class);
    static final Pattern VALUES_REGEX = Pattern.compile("[Vv][Aa][Ll][Uu][Ee][Ss]\\s*\\(");
    static final byte[] EMPTY_BYTES = new byte[0];
    public static final BytesCharSeq EMPTY_BYTES_CHAR_SEQ = new BytesCharSeq(EMPTY_BYTES);
    private static Field blockColumnsField;
    private static Field blockSettingsField;
    private static Field blockNameAndPositionsField;
    private static Field blockPlaceholderIndexesField;
    private static Field blockRowCntField;

    static {
        try {
            blockColumnsField = Block.class.getDeclaredField("columns");
            blockColumnsField.setAccessible(true);
            blockSettingsField = Block.class.getDeclaredField("settings");
            blockSettingsField.setAccessible(true);
            blockNameAndPositionsField = Block.class.getDeclaredField("nameAndPositions");
            blockNameAndPositionsField.setAccessible(true);
            blockPlaceholderIndexesField = Block.class.getDeclaredField("placeholderIndexes");
            blockPlaceholderIndexesField.setAccessible(true);
            blockRowCntField = Block.class.getDeclaredField("rowCnt");
            blockRowCntField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    /**
     * 转换host => jdbc url格式 192.168.40.222:9001,192.168.40.223:9001 =>
     * jdbc:clickhouse://192.168.40.222:9001,jdbc:clickhouse://192.168.40.223:9001
     */
    public static String[] buildUrlsFromHost(String host) {
        String[] hosts = host.split(",");
        List<String> urls = new ArrayList<>(hosts.length);
        for (int i = 0; i < hosts.length; i++) {
            String[] ipPort = hosts[i].trim().split(":");
            String ip = ipPort[0].trim();
            int port = Integer.parseInt(ipPort[1].trim());
            urls.add("jdbc:clickhouse://" + ip + ":" + port);
        }
        return urls.toArray(new String[urls.size()]);
    }

    public static String genePreparedInsertSql(String table, String[] columnNames) {
        StringBuilder sb = new StringBuilder("insert into ");
        sb.append(table).append("(");
        //sb.append(String.join(",", Arrays.stream(columnNames).map(x -> "`" + x +"`").collect(Collectors.toList())));
        sb.append(String.join(",", columnNames));
        sb.append(")");
        sb.append(" values(");
        sb.append(String.join(",", Arrays.stream(columnNames).map(x -> "?").collect(Collectors.toList())));
        sb.append(")");
        return sb.toString();
    }

    public static Tuple2<String[], Object[]> getInsertColumnsAndDefaultValuesForTable(
            String[] urls, int urlIndex, Properties connInfo, String table) throws Exception {
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");

        int retryCount = 0;
        while (true) {
            retryCount++;
            ClickHouseConnection connection = null;
            Statement stmt = null;
            ResultSet rst = null;
            try {
                connection = (ClickHouseConnection) DriverManager.getConnection(urls[urlIndex], connInfo);
                stmt = connection.createStatement();
                rst = stmt.executeQuery("desc " + table);

                List<String> columnNames = new ArrayList<>();
                List<Object> columnDefaultValues = new ArrayList<>();
                while (rst.next()) {
                    String name = rst.getString("name");
                    String typeStr = rst.getString("type");
                    String defaultTypeStr = rst.getString("default_type");
                    String defaultExpression = rst.getString("default_expression");
                    if ("LowCardinality(String)".equals(typeStr)) {
                        typeStr = "String";
                    }
                    IDataType<?, ?> type = DataTypeFactory.get(typeStr, connection.serverContext());
                    if ("MATERIALIZED".equals(defaultTypeStr)) {
                        continue;
                    }
                    Object defaultValue = parseDefaultValue(type, defaultExpression); // 只解析数字和字符串
                    if (defaultValue == null && !type.nullable()) {
                        if (type instanceof DataTypeArray) {
                            defaultValue = new ClickHouseArray(((DataTypeArray) type).getElemDataType(), new Object[0]);
                        } else {
                            defaultValue = type.defaultValue();
                        }
                    }
                    columnNames.add(name);
                    columnDefaultValues.add(defaultValue);
                }

                return new Tuple2<>(columnNames.toArray(new String[columnNames.size()]), columnDefaultValues.toArray(new Object[columnDefaultValues.size()]));
            } catch (SQLException e) {
                LOG.error("ClickHouse Connection Exception url:" + urls[urlIndex], e);
                if (retryCount >= 3) {
                    throw e;
                }
                urlIndex++;
                if (urlIndex == urls.length) {
                    urlIndex = 0;
                }
            } finally {
                if (rst != null) {
                    rst.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        }
    }

    public static ZoneId chooseTimeZone(String[] urls, int urlIndex, Properties connInfo) throws Exception {
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");

        int retryCount = 0;
        while (true) {
            retryCount++;
            Connection connection = null;
            try {
                connection = DriverManager.getConnection(urls[urlIndex], connInfo);
                ZoneId tz = DateTimeUtil.chooseTimeZone(((ClickHouseConnection) connection).serverContext());
                return tz;
            } catch (SQLException e) {
                LOG.error("ClickHouse Connection Exception url:" + urls[urlIndex], e);
                if (retryCount >= 3) {
                    throw e;
                }
                urlIndex++;
                if (urlIndex == urls.length) {
                    urlIndex = 0;
                }
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
        }
    }

    public static Block getInsertBlockForTable(String[] urls, int urlIndex, Properties connInfo, String table) throws Exception {
        String sql = "insert into " + table + " values";
        return getInsertBlockForSql(urls, urlIndex, connInfo, sql);
    }

    public static Block getInsertBlockForSql(String[] urls, int urlIndex, Properties connInfo, String sql) throws Exception {
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");

        Matcher matcher = VALUES_REGEX.matcher(sql);
        Preconditions.checkArgument(matcher.find(), "insert sql syntax error:%s", sql);
        String insertQuery = sql.substring(0,  matcher.end() - 1);
        LOG.warn("getInsertBlock insertQuery:{}.", insertQuery);

        int retryCount = 0;
        while (true) {
            retryCount++;
            Connection connection = null;
            try {
                connection = DriverManager.getConnection(urls[urlIndex], connInfo);
                Block block = ((ClickHouseConnection) connection).getSampleBlock(insertQuery);
                return block;
            } catch (SQLException e) {
                LOG.error("ClickHouse getInsertBlock Exception url:" + urls[urlIndex], e);
                if (retryCount >= 3) {
                    throw e;
                }
                urlIndex++;
                if (urlIndex == urls.length) {
                    urlIndex = 0;
                }
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
        }
    }

    public static Block newInsertBlockFrom(Block block) throws Exception {
        int rowCnt = 0; // (int) blockRowCntField.get(block);
        int columnCnt = block.columnCnt();
        BlockSettings settings = (BlockSettings) blockSettingsField.get(block);
        IColumn[] srcColumns = (IColumn[]) blockColumnsField.get(block);
        IColumn[] columns = new IColumn[columnCnt];

        for (int i = 0; i < columnCnt; i++) {
            String name = srcColumns[i].name();
            IDataType<?, ?> dataType = srcColumns[i].type();
            columns[i] = ColumnFactory.createColumn(name, dataType, null); // values用于rst读取
        }

        Block newBlock = new Block(rowCnt, columns, settings);
        return newBlock;
    }

    public static void copyInsertBlockColumns(Block src, Block desc) throws Exception {
        desc.cleanup();

        IColumn[] srcColumns = (IColumn[]) blockColumnsField.get(src);
        IColumn[] descColumns = (IColumn[]) blockColumnsField.get(desc);
        for (int i = 0; i < srcColumns.length; i++) {
            descColumns[i] = srcColumns[i];
        }

        blockRowCntField.set(desc, blockRowCntField.get(src));
    }

    public static void resetInsertBlockColumns(Block block) throws Exception {
        block.cleanup();
        blockRowCntField.set(block, 0);

        IColumn[] columns = (IColumn[]) blockColumnsField.get(block);
        for (int i = 0; i < columns.length; i++) {
            String name = columns[i].name();
            IDataType<?, ?> dataType = columns[i].type();
            columns[i] = ColumnFactory.createColumn(name, dataType, null); // values用于rst读取
        }

        block.initWriteBuffer();
    }

    private static Object parseDefaultValue(IDataType<?, ?> type, String defaultExpression) {
        Object defaultValue = null;
        if (!StringUtils.isBlank(defaultExpression)) {
            if (type instanceof DataTypeString || type instanceof DataTypeFixedString) {
                defaultValue = defaultExpression;
            } else if (type instanceof DataTypeInt8) {
                defaultValue = (byte) Integer.parseInt(defaultExpression);
            } else if (type instanceof DataTypeUInt8 || type instanceof DataTypeInt16) {
                defaultValue = (short) Integer.parseInt(defaultExpression);
            } else if (type instanceof DataTypeUInt16 || type instanceof DataTypeInt32) {
                defaultValue = Integer.parseInt(defaultExpression);
            } else if (type instanceof DataTypeUInt32 || type instanceof DataTypeInt64) {
                defaultValue = Long.parseLong(defaultExpression);
            } else if (type instanceof DataTypeUInt64) {
                defaultValue = BigInteger.valueOf(Long.parseLong(defaultExpression));
            } else if (type instanceof DataTypeFloat32) {
                defaultValue = Float.parseFloat(defaultExpression);
            } else if (type instanceof DataTypeFloat64) {
                defaultValue = Double.parseDouble(defaultExpression);
            } else if (type instanceof DataTypeDecimal) {
                defaultValue = new BigDecimal(Double.parseDouble(defaultExpression));
            }
        }

        return defaultValue;
    }

    // 仅用于测试
    public static void showBlockColumnsByteSize(Block block) throws Exception {
        IColumn[] columns = (IColumn[]) blockColumnsField.get(block);
        Field field = AbstractColumn.class.getDeclaredField("buffer");
        field.setAccessible(true);
        Field columnWriterField = ColumnWriterBuffer.class.getDeclaredField("columnWriter");
        columnWriterField.setAccessible(true);
        Field byteBufferListField = ByteArrayWriter.class.getDeclaredField("byteBufferList");
        byteBufferListField.setAccessible(true);
        int size = 0;
        int totalSize = 0;
        double unitM = 1 << 20;
        LOG.warn("rowCount:" + block.rowCnt());
        for (int i = 0; i < columns.length; i++) {
            Object columnWriter = columnWriterField.get(field.get(columns[i]));
            List<ByteBuffer> byteBufferList =
                    (List<ByteBuffer>) byteBufferListField.get(columnWriter);
            size = 0;
            for (ByteBuffer byteBuffer : byteBufferList) {
                size += byteBuffer.position();
            }
            totalSize += size;
            if (size > unitM) {
                // LOG.warn(columns[i].name() + "buf cnt:" + byteBufferList.size() +  ", size:" +
                // size/unitM + "M");
            }
        }
        LOG.warn("totalSize:" + totalSize / unitM + "M");
    }
}
