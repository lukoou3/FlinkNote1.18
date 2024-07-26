package com.java.flink.connector.clickhouse.util;

import com.java.flink.connector.clickhouse.buffer.BufferPool;
import com.java.flink.connector.clickhouse.buffer.ReusedColumnWriterBuffer;
import com.java.flink.connector.clickhouse.jdbc.DataTypeStringV2;
import com.github.housepower.data.*;
import com.github.housepower.data.type.*;
import com.github.housepower.data.type.complex.*;
import com.github.housepower.jdbc.ClickHouseArray;
import com.github.housepower.jdbc.ClickHouseConnection;
import com.github.housepower.misc.BytesCharSeq;
import com.github.housepower.misc.DateTimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.ZoneId;
import java.util.*;
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

    public static Tuple3<String[], Object[], int[]> getInsertColumnsAndDefaultValuesAndDefaultSizesForTable(
            String[] urls, int urlIndex, Properties connInfo, String table, @Nullable String[] insertColumns) throws Exception {
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");

        boolean insertColumnsPresent = insertColumns != null;
        Set<String> insertColumnSet = insertColumnsPresent ? Arrays.stream(insertColumns).collect(Collectors.toSet()):null;
        Preconditions.checkArgument(!insertColumnsPresent || insertColumnSet.size() == insertColumns.length, "column names must be unique");

        List<Tuple3<String, Object, Integer>> columnInfos = new ArrayList<>();
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
                while (rst.next()) {
                    String name = rst.getString("name");
                    String typeStr = rst.getString("type");
                    String defaultTypeStr = rst.getString("default_type");
                    String defaultExpression = rst.getString("default_expression");
                    if (insertColumnsPresent && !insertColumnSet.contains(name)) {
                        continue;
                    }
                    if ("LowCardinality(String)".equals(typeStr)) {
                        typeStr = "String";
                    }
                    if ("MATERIALIZED".equals(defaultTypeStr)) {
                        continue;
                    }
                    IDataType<?, ?> type = DataTypeFactory.get(typeStr, connection.serverContext());

                    Object defaultValue = parseDefaultValue(type, defaultExpression); // 只解析数字和字符串
                    if (defaultValue == null && !type.nullable()) {
                        if (type instanceof DataTypeArray) {
                            defaultValue = new ClickHouseArray(((DataTypeArray) type).getElemDataType(), new Object[0]);
                        } else if (type instanceof DataTypeMap) {
                            defaultValue = Collections.emptyMap();
                        } else {
                            defaultValue = type.defaultValue();
                        }
                    }
                    columnInfos.add(Tuple3.of(name, defaultValue, getDefaultValueSize(type, defaultExpression)));
                }

                Map<String, Tuple3<String, Object, Integer>> columnInfoMap = columnInfos.stream().collect(Collectors.toMap(x -> x.f0, x -> x));
                String[] columnNames = insertColumnsPresent?insertColumns:columnInfos.stream().map(x->x.f0).toArray(String[]::new);
                Object[] columnDefaultValues = new Object[columnNames.length];
                int[] columnDefaultSizes = new int[columnNames.length];
                for (int i = 0; i < columnNames.length; i++) {
                    Tuple3<String, Object, Integer> columnInfo = columnInfoMap.get(columnNames[i]);
                    Preconditions.checkNotNull(columnInfo, "not exist column:" + columnNames[i]);
                    columnDefaultValues[i] = columnInfo.f1;
                    columnDefaultSizes[i] = columnInfo.f2;
                }
                return Tuple3.of(columnNames, columnDefaultValues, columnDefaultSizes);
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

        String insertQuery;
        if (sql.trim().toLowerCase().endsWith("values")) {
            insertQuery = sql;
        } else {
            Matcher matcher = VALUES_REGEX.matcher(sql);
            Preconditions.checkArgument(matcher.find(), "insert sql syntax error:%s", sql);
            insertQuery = sql.substring(0,  matcher.end() - 1);
        }
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
        //desc.cleanup();

        IColumn[] srcColumns = (IColumn[]) blockColumnsField.get(src);
        IColumn[] descColumns = (IColumn[]) blockColumnsField.get(desc);
        for (int i = 0; i < srcColumns.length; i++) {
            descColumns[i] = srcColumns[i];
        }

        blockRowCntField.set(desc, blockRowCntField.get(src));
    }

    public static void initBlockWriteBuffer(Block block, BufferPool bufferPool) throws Exception {
        IColumn[] columns = (IColumn[]) blockColumnsField.get(block);
        for (int i = 0; i < columns.length; i++) {
            ColumnWriterBuffer writeBuffer = columns[i].getColumnWriterBuffer();
            if(writeBuffer == null){
                writeBuffer = new ReusedColumnWriterBuffer(bufferPool);
                columns[i].setColumnWriterBuffer(writeBuffer);
            }else{
                writeBuffer.reset();
            }
        }
    }

    public static void resetInsertBlockColumns(Block block) throws Exception {
        blockRowCntField.set(block, 0);

        IColumn[] columns = (IColumn[]) blockColumnsField.get(block);
        for (int i = 0; i < columns.length; i++) {
            ColumnWriterBuffer writeBuffer = columns[i].getColumnWriterBuffer();
            String name = columns[i].name();
            IDataType<?, ?> dataType = columns[i].type();
            columns[i] = ColumnFactory.createColumn(name, dataType, null); // values用于rst读取
            writeBuffer.reset();
            columns[i].setColumnWriterBuffer(writeBuffer);
        }
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

    private static int getDefaultValueSize(IDataType<?, ?> type, String defaultExpression){
        if (type instanceof DataTypeString || type instanceof DataTypeFixedString || type instanceof DataTypeStringV2) {
            if(StringUtils.isBlank(defaultExpression)){
                return  1;
            }else{
                return writeBytesSizeByLen(defaultExpression.getBytes(StandardCharsets.UTF_8).length);
            }
        }

        if (type instanceof DataTypeDate) {
            return 2;
        }

        if (type instanceof DataTypeDate32) {
            return 4;
        }

        if (type instanceof DataTypeDateTime) {
            return 4;
        }

        if (type instanceof DataTypeDateTime64) {
            return 8;
        }

        if (type instanceof DataTypeInt8 || type instanceof DataTypeUInt8) {
            return 1;
        }

        if (type instanceof DataTypeInt16 || type instanceof DataTypeUInt16) {
            return 2;
        }

        if (type instanceof DataTypeInt32 || type instanceof DataTypeUInt32) {
            return 4;
        }

        if (type instanceof DataTypeInt64 || type instanceof DataTypeUInt64) {
            return 8;
        }

        if (type instanceof DataTypeFloat32) {
            return 4;
        }

        if (type instanceof DataTypeFloat64) {
            return 8;
        }

        if (type instanceof DataTypeDecimal) {
            return 32;
        }

        if (type instanceof DataTypeUUID) {
            return 16;
        }

        if (type instanceof DataTypeNothing) {
            return 1;
        }

        if (type instanceof DataTypeNullable) {
            return getDefaultValueSize(((DataTypeNullable)type).getNestedDataType(), defaultExpression);
        }

        if (type instanceof DataTypeArray) {
            return 0;
        }

        if (type instanceof DataTypeMap) {
            return 0;
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    public static int writeBytesSizeByLen(final int len) {
        int bytes = 1, value = len;
        while ((value & 0xffffff80) != 0L) {
            bytes += 1;
            value >>>= 7;
        }
        return bytes + len;
    }

    public static BlockColumnsByteSizeInfo getBlockColumnsByteSizeInfo(Block block) throws Exception {
        IColumn[] columns = (IColumn[]) blockColumnsField.get(block);
        long size = 0, bufferSize = 0;
        long totalSize = 0, totalBufferSize = 0;
        long sizeThreshold = Math.max(200 << 20 / columns.length * 2, 4 << 20);
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < columns.length; i++) {
            List<ByteBuffer> byteBufferList = ((ReusedColumnWriterBuffer) columns[i].getColumnWriterBuffer()).getBufferList();
            size = 0;
            bufferSize = 0;
            for (ByteBuffer byteBuffer : byteBufferList) {
                size += byteBuffer.position();
                bufferSize += byteBuffer.capacity();
            }
            totalSize += size;
            totalBufferSize += bufferSize;
            if (bufferSize > sizeThreshold) {
                if(sb.length() > 0){
                    sb.append(',');
                }
                sb.append(String.format("%s:%d M", columns[i].name(), (bufferSize >>> 20)) );
            }
        }

        return new BlockColumnsByteSizeInfo(totalSize, totalBufferSize, sb.toString());
    }


}
