package com.java.flink.connector.clickhouse.sink;

import com.alibaba.fastjson2.JSON;
import com.java.flink.connector.clickhouse.buffer.BufferPool;
import com.java.flink.connector.clickhouse.jdbc.BytesCharVarSeq;
import com.java.flink.connector.clickhouse.jdbc.ClickHouseBatchInsertConnection;
import com.java.flink.connector.clickhouse.jdbc.ClickHousePreparedBatchInsertStatement;
import com.java.flink.connector.clickhouse.jdbc.DataTypeStringV2;
import com.java.flink.connector.clickhouse.metrics.InternalMetrics;
import com.java.flink.connector.clickhouse.util.BlockColumnsByteSizeInfo;
import com.java.flink.connector.clickhouse.util.ClickHouseUtils;
import com.github.housepower.data.*;
import com.github.housepower.data.type.*;
import com.github.housepower.data.type.complex.*;
import com.github.housepower.exception.ClickHouseSQLException;
import com.github.housepower.jdbc.ClickHouseArray;
import com.github.housepower.misc.BytesCharSeq;
import com.github.housepower.misc.DateTimeUtil;
import com.github.housepower.settings.SettingKey;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.java.flink.connector.clickhouse.util.ClickHouseUtils.writeBytesSizeByLen;

public abstract class AbstractBatchIntervalClickHouseSink<T> extends RichSinkFunction<T>
        implements CheckpointedFunction {
    static final Logger LOG = LoggerFactory.getLogger(AbstractBatchIntervalClickHouseSink.class);
    public static long UINT32_MAX = (1L << 32) - 1;
    // 标准日期时间格式，精确到秒：yyyy-MM-dd HH:mm:ss
    public static final String NORM_DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter NORM_DATETIME_FORMATTER = DateTimeFormatter.ofPattern(NORM_DATETIME_PATTERN);
    // 标准日期时间格式，精确到毫秒：yyyy-MM-dd HH:mm:ss.SSS
    public static final String NORM_DATETIME_MS_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final DateTimeFormatter NORM_DATETIME_MS_FORMATTER = DateTimeFormatter.ofPattern(NORM_DATETIME_MS_PATTERN);
    public static final DateTimeFormatter DATETIME_FORMATTER = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
    private final int batchSize;
    private final int batchByteSize;
    private final long batchIntervalMs;
    private transient volatile boolean closed;
    private transient InternalMetrics internalMetrics;
    private transient Thread outThread;
    private transient ReentrantLock lock;
    private transient Block batch;
    private int batchWriteByteSize;
    private transient BlockingQueue<Block> outBatchQueue;
    private transient BlockingQueue<Block> freeBatchQueue;
    private transient BufferPool bufferPool;
    private transient Exception flushException;
    private transient long lastFlushTs;
    // flush ck 相关
    private final String[] urls;
    private int urlIndex;
    private final Properties connInfo;
    private final String table;
    private final String[] insertColumns;
    private String insertSql;
    protected ZoneId tz;
    protected String[] columnNames;
    protected Object[] columnDefaultValues;
    protected int[] columnDefaultSizes;
    protected IDataType<?, ?>[] columnTypes;
    protected ValueConverter[] columnConverters;
    protected final SizeHelper writeSizeHelper;

    public AbstractBatchIntervalClickHouseSink(int batchSize, int batchByteSize, long batchIntervalMs, String host, String table, Properties connInfo) {
        this(batchSize, batchByteSize, batchIntervalMs, host, table, null, connInfo);
    }

    public AbstractBatchIntervalClickHouseSink(int batchSize, int batchByteSize, long batchIntervalMs, String host, String table, @Nullable String[] insertColumns, Properties connInfo) {
        this.batchSize = batchSize;
        this.batchByteSize = batchByteSize;
        this.batchIntervalMs = batchIntervalMs;
        this.writeSizeHelper = new SizeHelper();
        this.urls = ClickHouseUtils.buildUrlsFromHost(host);
        this.table = table;
        this.insertColumns = insertColumns;
        this.connInfo = connInfo;
        if(!this.connInfo.containsKey(SettingKey.connect_timeout.name())){
            this.connInfo.setProperty(SettingKey.connect_timeout.name(), "30");
        }if(!this.connInfo.containsKey(SettingKey.query_timeout.name())){
            this.connInfo.setProperty(SettingKey.query_timeout.name(), "300");
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        initMetric();
        lock = new ReentrantLock();
        outBatchQueue = new LinkedBlockingQueue<>(1);
        freeBatchQueue = new LinkedBlockingQueue<>(2);
        initClickHouseParams();
        onInit(parameters);
        lastFlushTs = System.currentTimeMillis();
        final String threadName = "BatchIntervalSink-" + (getRuntimeContext().getIndexOfThisSubtask() + 1) + "/" + getRuntimeContext().getNumberOfParallelSubtasks();
        outThread = new Thread(() -> {
            while (!closed) {
                try {
                    Block list;
                    try {
                        list = outBatchQueue.poll(2, TimeUnit.SECONDS);
                        if (list == null) {
                            if (System.currentTimeMillis() - lastFlushTs >= batchIntervalMs) {
                                lock.lock();
                                try {
                                    // 正常情况应该是一个线程生产，一个消费。防止极限情况生产线程刚好生产，造成死锁。
                                    if(outBatchQueue.isEmpty()){
                                        flush();
                                    }
                                } finally {
                                    lock.unlock();
                                }
                            }
                            continue;
                        }
                    } catch (InterruptedException e) {
                        continue;
                    }

                    doFlushAndResetBlock(list, true);
                } catch (Throwable e) {
                    LOG.error("BatchIntervalSinkThreadError", e);
                    flushException = new Exception("BatchIntervalSinkThreadError", e);
                }
            }
        }, threadName);
        outThread.start();
    }

    private void initMetric() throws Exception {
        internalMetrics = new InternalMetrics(getRuntimeContext());
    }

    private void initClickHouseParams() throws Exception {
        // urlIndex = new Random().nextInt(urls.length);
        urlIndex = getRuntimeContext().getIndexOfThisSubtask() % urls.length;

        // 获取要插入的列信息
        Tuple3<String[], Object[], int[]> columnsAndDefaultValuesAndDefaultSizes = ClickHouseUtils.getInsertColumnsAndDefaultValuesAndDefaultSizesForTable(urls, urlIndex, connInfo, table, insertColumns);
        columnNames = columnsAndDefaultValuesAndDefaultSizes.f0;
        columnDefaultValues = columnsAndDefaultValuesAndDefaultSizes.f1;
        columnDefaultSizes = columnsAndDefaultValuesAndDefaultSizes.f2;
        insertSql = ClickHouseUtils.genePreparedInsertSql(this.table, this.columnNames);

        LOG.warn("insertColumnsCount:" + columnNames.length);
        LOG.warn("insertColumns:" + String.join(",", columnNames));
        LOG.warn("insertColumnDefaultValuesAndSizes:" + IntStream.range(0, columnNames.length).mapToObj(i -> columnNames[i] + ":" + columnDefaultValues[i] + "(" + columnDefaultSizes[i] + ")").collect(Collectors.joining(",")));
        LOG.warn("insertSql:" + insertSql);

        // 获取时区用于解析DateTime类型
        tz = ClickHouseUtils.chooseTimeZone(urls, urlIndex, connInfo);

        bufferPool = new BufferPool(0, 1024L * 1024 * Math.max(columnNames.length * 2, 200), 1000 * 60 * 20);

        // 初始化Block
        Block block = ClickHouseUtils.getInsertBlockForSql(urls, urlIndex, connInfo, insertSql);
        assert block.columnCnt() == columnNames.length;
        ClickHouseUtils.initBlockWriteBuffer(block, bufferPool); // block.initWriteBuffer();
        freeBatchQueue.put(block);

        Field typeField = AbstractColumn.class.getDeclaredField("type");
        typeField.setAccessible(true);
        columnTypes = new IDataType<?, ?>[block.columnCnt()];
        for (int i = 0; i < columnNames.length; i++) {
            IColumn column = block.getColumn(i);
            columnTypes[i] = column.type();
            assert columnNames[i].equals(column.name());

            if (column.type() instanceof DataTypeString
                    || column.type() instanceof DataTypeFixedString) {
                if (columnDefaultValues[i].equals("")) {
                    columnDefaultValues[i] = ClickHouseUtils.EMPTY_BYTES_CHAR_SEQ;
                } else {
                    columnDefaultValues[i] =
                            new BytesCharSeq((columnDefaultValues[i].toString().getBytes(StandardCharsets.UTF_8)));
                }
            }

            if (column.type() instanceof DataTypeString && column instanceof Column) {
                DataTypeStringV2 dataTypeStringV2 = new DataTypeStringV2(StandardCharsets.UTF_8);
                typeField.set(column, dataTypeStringV2);
                columnTypes[i] = dataTypeStringV2;
            }
        }

        columnConverters = Arrays.stream(columnTypes).map(this::makeConverter).toArray(ValueConverter[]::new);

        // 从block复制block
        block = ClickHouseUtils.newInsertBlockFrom(block);
        ClickHouseUtils.initBlockWriteBuffer(block, bufferPool); // block.initWriteBuffer();
        freeBatchQueue.put(block);
        batch = freeBatchQueue.take();
    }

    public final void checkFlushException() throws Exception {
        if (flushException != null) throw flushException;
    }

    protected void onInit(Configuration parameters) throws Exception {}
    protected void onClose() throws Exception {}

    protected abstract int addBatch(Block batch, T data) throws Exception;

    @Override
    public final void invoke(T value, Context context) throws Exception {
        checkFlushException();
        internalMetrics.incrementInEvents();

        lock.lock();
        try {
            int writeSize = addBatch(batch, value);
            if (writeSize > 0) {
                batch.appendRow();
                batchWriteByteSize += writeSize;
            }
            if (batch.rowCnt() >= batchSize || batchWriteByteSize >= batchByteSize) {
                // LOG.warn("flush");
                flush();
            }
        } catch (Exception e) {
            internalMetrics.incrementErrorEvents();
            LOG.error("转换ck类型异常", e);
        } finally{
            lock.unlock();
        }
    }

    public final void flush() throws Exception {
        checkFlushException();
        lock.lock();
        try {
            if (batch.rowCnt() <= 0) {
                return;
            }
            outBatchQueue.put(batch);
            batch = freeBatchQueue.take();
            batchWriteByteSize = 0;
        } finally {
            lock.unlock();
        }
    }

    private void doFlushAndResetBlock(Block block, boolean recycle) throws Exception {
        try {
            doFlush(block);
        }finally {
            ClickHouseUtils.resetInsertBlockColumns(block);
            if(recycle){
                // 必须保证放入freeBatchQueue，否则发生异常没放入freeBatchQueue会导致block丢失，freeBatchQueue.take()申请block时会一直阻塞
                freeBatchQueue.put(block);
                int mebiBytes = (int) (bufferPool.getCurrentCacheSize() >>> 20);
                if(mebiBytes >= 150){
                    LOG.warn("bufferPoolCacheBufferSize: " + mebiBytes + "M");
                }
            }
        }
    }

    private void doFlush(Block block) throws Exception {
        long start = System.currentTimeMillis();
        int rowCnt = block.rowCnt();
        BlockColumnsByteSizeInfo blockByteSizeInfo = ClickHouseUtils.getBlockColumnsByteSizeInfo(block);
        LOG.warn("flush " + rowCnt + ", totalSize:" + (blockByteSizeInfo.totalSize >>> 20)  + "M, totalBufferSize: " + (blockByteSizeInfo.totalBufferSize >>> 20) +  "M, start:" + new Timestamp(start) + "," + (start - lastFlushTs));
        if(!blockByteSizeInfo.bigColumnsInfo.isEmpty()){
            LOG.warn("bigColumnsInfo:" + blockByteSizeInfo.bigColumnsInfo);
        }
        lastFlushTs = System.currentTimeMillis();

        int retryCount = 0;
        while (true) {
            retryCount++;

            String url = urls[urlIndex];
            urlIndex++;
            if (urlIndex == urls.length) {
                urlIndex = 0;
            }

            ClickHouseBatchInsertConnection connection = null;
            ClickHousePreparedBatchInsertStatement stmt = null;
            try {
                connection = ClickHouseBatchInsertConnection.connect(url, connInfo);
                stmt = connection.prepareStatement(insertSql);
                ClickHouseUtils.copyInsertBlockColumns(block, stmt.getBlock());
                stmt.executeBatch();

                internalMetrics.incrementOutEvents(rowCnt);
                internalMetrics.incrementOutBytes(blockByteSizeInfo.totalSize);
                LOG.warn("flush " + rowCnt + " end:" + new Timestamp(System.currentTimeMillis()) + "," + (System.currentTimeMillis() - start));

                return;
            } catch (Exception e) {
                LOG.error("ClickHouseBatchInsertFail url:" + url, e);
                if (retryCount >= 3) {
                    internalMetrics.incrementErrorEvents(rowCnt);
                    LOG.error("ClickHouseBatchInsertFinalFail for rowCnt:" + rowCnt);
                    // throw e;
                    return;
                }
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Exception e) {
                        LOG.error("ClickHouseBatchInsertFail url:" + url, e);
                        if (retryCount >= 3) {
                            LOG.error("ClickHouseBatchInsertFinalFail for rowCnt:" + rowCnt);
                            //throw e;
                            closeQuietly(connection);
                            return;
                        }
                    }
                }
                closeQuietly(connection);
            }
        }
    }

    public static void closeQuietly(ClickHouseBatchInsertConnection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.error("ClickHouseConnectionCloseError:", e);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {}

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {}

    @Override
    public final void close() throws Exception {
        if (!closed) {
            LOG.warn("ck_sink_close_start");
            closed = true;

            if (outThread != null) {
                try {
                    outThread.join();
                } catch (Exception e) {

                }
            }

            // init中可能抛出异常
            if (lock != null) {
                lock.lock();
                try {
                    // batch init中可能抛出异常
                    if (batch != null && batch.rowCnt() > 0) {
                        doFlushAndResetBlock(batch, false);
                    }
                    // 缓存的Block不用归还释放列IColumn申请的ColumnWriterBuffer，会被gc。
                    // ConcurrentLinkedDeque<ColumnWriterBuffer> stack 缓存池没有记录列表总大小，使用大小等信息，没限制列表大小。不归还ColumnWriterBuffer没问题。
                } catch (Throwable t) {
                    if(t instanceof Exception){
                        flushException = (Exception) t;
                    }else{
                        flushException = new Exception(t);
                    }
                } finally {
                    lock.unlock();
                }
            }

            onClose();

            LOG.warn("ck_sink_close_end");
        }

        checkFlushException();
    }

    protected ValueConverter makeConverter(IDataType<?, ?> type) {
        return makeConverter(type, true);
    }

    protected ValueConverter makeConverter(IDataType<?, ?> type, boolean objectReuse) {
        // put the most common cast at first to avoid `instanceof` test overhead
        if (type instanceof DataTypeString || type instanceof DataTypeFixedString) {
            return makeStringConverter();
        }

        if (type instanceof DataTypeStringV2) {
            return makeStringV2Converter();
        }

        if (type instanceof DataTypeDate) {
            return this::convertDate;
        }

        if (type instanceof DataTypeDate32) {
            return this::convertDate32;
        }

        if (type instanceof DataTypeDateTime) {
            return this::convertDateTime;
        }

        if (type instanceof DataTypeDateTime64) {
            return this::convertDateTime64;
        }

        if (type instanceof DataTypeInt8) {
            return this::convertInt8;
        }

        if (type instanceof DataTypeUInt8) {
            return this::convertUInt8;
        }

        if (type instanceof DataTypeInt16) {
            return this::convertInt16;
        }

        if (type instanceof DataTypeUInt16) {
            return this::convertUInt16;
        }

        if (type instanceof DataTypeInt32) {
            return this::convertInt32;
        }

        if (type instanceof DataTypeUInt32) {
            return this::convertUInt32;
        }

        if (type instanceof DataTypeInt64) {
            return this::convertInt64;
        }

        if (type instanceof DataTypeUInt64) {
            return this::convertUInt64;
        }

        if (type instanceof DataTypeFloat32) {
            return this::convertFloat32;
        }

        if (type instanceof DataTypeFloat64) {
            return this::convertFloat64;
        }

        if (type instanceof DataTypeDecimal) {
            return this::convertDecimal;
        }

        if (type instanceof DataTypeUUID) {
            return this::convertUUID;
        }

        if (type instanceof DataTypeNothing) {
            return this::convertNothing;
        }

        if (type instanceof DataTypeNullable) {
            IDataType nestedDataType = ((DataTypeNullable) type).getNestedDataType();
            ValueConverter converter = this.makeConverter(nestedDataType);
            return (obj, sizeHelper) -> {
                if (obj == null) {
                    return null;
                }
                return converter.convert(obj, sizeHelper);
            };
        }

        if (type instanceof DataTypeArray) {
            IDataType<?, ?> eleDataType = ((DataTypeArray) type).getElemDataType();
            ValueConverter eleConverter = this.makeConverter(eleDataType, false);
            Object defaultValue = new ClickHouseArray(eleDataType, new Object[0]);
            return (obj, sizeHelper) -> {
                return this.convertArray(obj, eleDataType, eleConverter, defaultValue, sizeHelper);
            };
        }

        if (type instanceof DataTypeMap) {
            IDataType<?, ?>[] kvTypes = ((DataTypeMap) type).getNestedTypes();
            return makeMapConverter(kvTypes, objectReuse);
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    private static final int MAX_STR_BYTES_LENGTH = 1024 * 12;

    private ValueConverter makeStringV2Converter() {
        return new ValueConverter() {
            final byte[] bytes = new byte[MAX_STR_BYTES_LENGTH];
            final BytesCharVarSeq bytesCharVarSeq = new BytesCharVarSeq(bytes, 0);

            @Override
            public Object convert(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
                if (obj == null) {
                    throw new ClickHouseSQLException(-1, "type doesn't support null value");
                }
                if (obj instanceof byte[]) {
                    byte[] bs = (byte[]) obj;
                    sizeHelper.size += writeBytesSizeByLen(bs.length);
                    bytesCharVarSeq.setBytesAndLen(bs, bs.length);
                    return bytesCharVarSeq;
                }
                String str;
                if (obj instanceof CharSequence) {
                    if (((CharSequence) obj).length() == 0) {
                        sizeHelper.size += 1;
                        return ClickHouseUtils.EMPTY_BYTES_CHAR_SEQ;
                    }
                    str = obj.toString();
                } else {
                    // LOG.debug("set value[{}]: {} on String Column", obj.getClass(), obj);
                    str = JSON.toJSONString(obj);
                }
                int length = str.length() * 3;
                byte[] bs = bytes;
                if (length > MAX_STR_BYTES_LENGTH) {
                    bs = new byte[length];
                }
                int len = encodeUTF8(str, bs);
                sizeHelper.size += writeBytesSizeByLen(len);
                bytesCharVarSeq.setBytesAndLen(bs, len);
                return bytesCharVarSeq;
            }
        };
    }

    private ValueConverter makeStringConverter() {
        return new ValueConverter() {
            final byte[] bytes = new byte[MAX_STR_BYTES_LENGTH];

            @Override
            public Object convert(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
                if (obj == null) {
                    throw new ClickHouseSQLException(-1, "type doesn't support null value");
                }
                if (obj instanceof byte[]) {
                    byte[] bs = (byte[]) obj;
                    sizeHelper.size += writeBytesSizeByLen(bs.length);
                    return new BytesCharSeq(bs);
                }
                String str;
                if (obj instanceof CharSequence) {
                    if (((CharSequence) obj).length() == 0) {
                        sizeHelper.size += 1;
                        return ClickHouseUtils.EMPTY_BYTES_CHAR_SEQ;
                    }
                    str = obj.toString();
                } else {
                    // LOG.debug("set value[{}]: {} on String Column", obj.getClass(), obj);
                    str = JSON.toJSONString(obj);
                }
                int length = str.length() * 3;
                byte[] bs = bytes;
                if (length > MAX_STR_BYTES_LENGTH) {
                    bs = new byte[length];
                }
                int len = encodeUTF8(str, bs);
                sizeHelper.size += writeBytesSizeByLen(len);
                return new BytesCharSeq(Arrays.copyOf(bytes, len));
            }
        };
    }

    private Object convertDate(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 2;
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof java.util.Date) return ((Date) obj).toLocalDate();
        if (obj instanceof LocalDate) return obj;

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertDate32(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 4;
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof java.util.Date) return ((Date) obj).toLocalDate();
        if (obj instanceof LocalDate) return obj;

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertDateTime(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 4;
        if (obj instanceof Number) {
            long ts = ((Number) obj).longValue();
            // 小于UINT32_MAX认为单位是s
            if (ts < UINT32_MAX) {
                ts = ts * 1000;
            }
            Instant instant = Instant.ofEpochMilli(ts);
            return LocalDateTime.ofInstant(instant, tz).atZone(tz);
        }

        if (obj instanceof Timestamp) return DateTimeUtil.toZonedDateTime((Timestamp) obj, tz);
        if (obj instanceof LocalDateTime) return ((LocalDateTime) obj).atZone(tz);
        if (obj instanceof ZonedDateTime) return obj;
        if (obj instanceof String) {
            String str = (String) obj;
            if (str.length() == 19) {
                return LocalDateTime.parse(str, NORM_DATETIME_FORMATTER).atZone(tz);
            } else if (str.length() == 23) {
                return LocalDateTime.parse(str, NORM_DATETIME_MS_FORMATTER).atZone(tz);
            } else if (str.length() >= 19) {
                return LocalDateTime.parse(str, DATETIME_FORMATTER).atZone(tz);
            }
        }

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertDateTime64(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 8;
        if (obj instanceof Number) {
            long ts = ((Number) obj).longValue();
            // 小于UINT32_MAX认为单位是s
            if (ts < UINT32_MAX) {
                ts = ts * 1000;
            }
            Instant instant = Instant.ofEpochMilli(ts);
            return LocalDateTime.ofInstant(instant, tz).atZone(tz);
        }

        if (obj instanceof Timestamp) return DateTimeUtil.toZonedDateTime((Timestamp) obj, tz);
        if (obj instanceof LocalDateTime) return ((LocalDateTime) obj).atZone(tz);
        if (obj instanceof ZonedDateTime) return obj;
        if (obj instanceof String) {
            String str = (String) obj;
            if (str.length() == 19) {
                return LocalDateTime.parse(str, NORM_DATETIME_FORMATTER).atZone(tz);
            } else if (str.length() == 23) {
                return LocalDateTime.parse(str, NORM_DATETIME_MS_FORMATTER).atZone(tz);
            } else if (str.length() >= 19) {
                return LocalDateTime.parse(str, DATETIME_FORMATTER).atZone(tz);
            }
        }

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertInt8(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 1;
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number) return ((Number) obj).byteValue();
        if (obj instanceof String) return (byte) Integer.parseInt((String) obj);

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertUInt8(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 1;
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number) return ((Number) obj).shortValue();
        if (obj instanceof String) return (short) Integer.parseInt((String) obj);

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertInt16(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 2;
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number) return ((Number) obj).shortValue();
        if (obj instanceof String) return (short) Integer.parseInt((String) obj);

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertUInt16(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 2;
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number) return ((Number) obj).intValue();
        if (obj instanceof String) return Integer.parseInt((String) obj);

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertInt32(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 4;
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number) return ((Number) obj).intValue();
        if (obj instanceof String) return Integer.parseInt((String) obj);

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertUInt32(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 4;
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number) return ((Number) obj).longValue();
        if (obj instanceof String) return Long.parseLong((String) obj);

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertInt64(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 8;
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number) return ((Number) obj).longValue();
        if (obj instanceof String) return Long.parseLong((String) obj);

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertUInt64(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 8;
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof BigInteger) return obj;
        if (obj instanceof BigDecimal) return ((BigDecimal) obj).toBigInteger();
        if (obj instanceof Number) return BigInteger.valueOf(((Number) obj).longValue());
        if (obj instanceof String) return BigInteger.valueOf(Long.parseLong((String) obj));

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertFloat32(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 4;
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number) return ((Number) obj).floatValue();
        if (obj instanceof String) return Float.parseFloat((String) obj);

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertFloat64(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 8;
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number) return ((Number) obj).doubleValue();
        if (obj instanceof String) return Double.parseDouble((String) obj);

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertDecimal(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 32;
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof BigDecimal) return obj;
        if (obj instanceof BigInteger) return new BigDecimal((BigInteger) obj);
        if (obj instanceof Number) return new BigDecimal(((Number) obj).doubleValue());
        if (obj instanceof String) return new BigDecimal(Double.parseDouble((String) obj));

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertUUID(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 16;
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof UUID) return obj;
        if (obj instanceof String) {
            return UUID.fromString((String) obj);
        }

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertNothing(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
        sizeHelper.size += 1;
        return null;
    }

    private Object convertArray(
            Object obj,
            IDataType<?, ?> eleDataType,
            ValueConverter eleConverter,
            Object defaultValue,
            SizeHelper sizeHelper)
            throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof ClickHouseArray) {
            return obj;
        }
        if (obj instanceof List) {
            List list = (List) obj;
            if (list.size() == 0) {
                return defaultValue;
            }
            Object[] elements = new Object[list.size()];
            for (int i = 0; i < elements.length; i++) {
                elements[i] = eleConverter.convert(list.get(i), sizeHelper);
            }
            return new ClickHouseArray(eleDataType, elements);
        }

        throw new ClickHouseSQLException(-1, "require ClickHouseArray for column, but found " + obj.getClass());
    }

    private ValueConverter makeMapConverter(IDataType<?, ?>[] kvTypes, boolean objectReuse){
        final ValueConverter keyConverter = this.makeConverter(kvTypes[0]);
        final ValueConverter valueConverter = this.makeConverter(kvTypes[1], false);
        return new ValueConverter() {
            Map<Object, Object> map = objectReuse? new HashMap<>():null;
            @Override
            public Object convert(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException {
                if (obj == null) {
                    throw new ClickHouseSQLException(-1, "type doesn't support null value");
                }
                if (obj instanceof Map){
                    Map<?, ?> dataMap = (Map<?, ?>) obj;
                    if(dataMap.isEmpty()){
                        return dataMap;
                    }
                    Map<Object, Object> result;
                    if(!objectReuse || dataMap.size() > 100){
                        result = new HashMap<>();
                    } else {
                        result = map;
                        result.clear();
                    }

                    for (Map.Entry<?, ?> entry : dataMap.entrySet()) {
                        Object key = keyConverter.convert(entry.getKey(), sizeHelper);
                        Object value = valueConverter.convert(entry.getValue(), sizeHelper);
                        result.put(key, value);
                    }
                    return result;
                }

                throw new ClickHouseSQLException(-1, "require Map for column, but found " + obj.getClass());
            }
        };
    }

    // copy from org.apache.flink.table.runtime.util.StringUtf8Utils
    static int encodeUTF8(String str, byte[] bytes) {
        int offset = 0;
        int len = str.length();
        int sl = offset + len;
        int dp = 0;
        int dlASCII = dp + Math.min(len, bytes.length);

        // ASCII only optimized loop
        while (dp < dlASCII && str.charAt(offset) < '\u0080') {
            bytes[dp++] = (byte) str.charAt(offset++);
        }

        while (offset < sl) {
            char c = str.charAt(offset++);
            if (c < 0x80) {
                // Have at most seven bits
                bytes[dp++] = (byte) c;
            } else if (c < 0x800) {
                // 2 bytes, 11 bits
                bytes[dp++] = (byte) (0xc0 | (c >> 6));
                bytes[dp++] = (byte) (0x80 | (c & 0x3f));
            } else if (Character.isSurrogate(c)) {
                final int uc;
                int ip = offset - 1;
                if (Character.isHighSurrogate(c)) {
                    if (sl - ip < 2) {
                        uc = -1;
                    } else {
                        char d = str.charAt(ip + 1);
                        if (Character.isLowSurrogate(d)) {
                            uc = Character.toCodePoint(c, d);
                        } else {
                            // for some illegal character
                            // the jdk will ignore the origin character and cast it to '?'
                            // this acts the same with jdk
                            return defaultEncodeUTF8(str, bytes);
                        }
                    }
                } else {
                    if (Character.isLowSurrogate(c)) {
                        // for some illegal character
                        // the jdk will ignore the origin character and cast it to '?'
                        // this acts the same with jdk
                        return defaultEncodeUTF8(str, bytes);
                    } else {
                        uc = c;
                    }
                }

                if (uc < 0) {
                    bytes[dp++] = (byte) '?';
                } else {
                    bytes[dp++] = (byte) (0xf0 | ((uc >> 18)));
                    bytes[dp++] = (byte) (0x80 | ((uc >> 12) & 0x3f));
                    bytes[dp++] = (byte) (0x80 | ((uc >> 6) & 0x3f));
                    bytes[dp++] = (byte) (0x80 | (uc & 0x3f));
                    offset++; // 2 chars
                }
            } else {
                // 3 bytes, 16 bits
                bytes[dp++] = (byte) (0xe0 | ((c >> 12)));
                bytes[dp++] = (byte) (0x80 | ((c >> 6) & 0x3f));
                bytes[dp++] = (byte) (0x80 | (c & 0x3f));
            }
        }
        return dp;
    }

    static int defaultEncodeUTF8(String str, byte[] bytes) {
        try {
            byte[] buffer = str.getBytes("UTF-8");
            System.arraycopy(buffer, 0, bytes, 0, buffer.length);
            return buffer.length;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("encodeUTF8 error", e);
        }
    }

    @FunctionalInterface
    public interface ValueConverter extends Serializable {
        Object convert(Object obj, SizeHelper sizeHelper) throws ClickHouseSQLException;
    }

    public static class SizeHelper implements Serializable{
        public int size = 0;
    }
}
