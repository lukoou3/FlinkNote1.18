package com.java.flink.connector.jdbc;

import com.java.flink.connector.common.BatchIntervalSink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class BatchIntervalJdbcSink<T> extends BatchIntervalSink<T> {
    static final Logger LOG = LoggerFactory.getLogger(BatchIntervalJdbcSink.class);
    private final JdbcConnectionOptions connectionOptions;
    private final int maxRetries;
    private final PeriodExecSqlStrategy periodExecSqlStrategy;
    protected final boolean hasDelete;
    private transient Connection conn;
    private transient PreparedStatement stmt;
    private transient PreparedStatement deleteStmt;

    public BatchIntervalJdbcSink(JdbcConnectionOptions connectionOptions, JdbcExecutionOptions executionOptions){
        this(connectionOptions, executionOptions, false, null, false);
    }

    public BatchIntervalJdbcSink(JdbcConnectionOptions connectionOptions, JdbcExecutionOptions executionOptions, boolean keyedMode, PeriodExecSqlStrategy periodExecSqlStrategy, boolean hasDelete){
        super(executionOptions.getBatchSize(), executionOptions.getBatchIntervalMs(), executionOptions.getMinPauseBetweenFlushMs(), keyedMode);
        this.connectionOptions = connectionOptions;
        this.maxRetries = executionOptions.getMaxRetries();
        this.periodExecSqlStrategy = periodExecSqlStrategy;
        this.hasDelete = hasDelete;
    }

    protected abstract String getUpdateSql();

    protected abstract void setStmt(PreparedStatement stmt, T data) throws Exception;

    protected String getDeleteSql(){
        throw new RuntimeException("hasDelete时必须实现");
    }

    protected boolean isDeleteData(T data){
        throw new RuntimeException("hasDelete时必须实现");
    }

    protected void setDeleteStmt(PreparedStatement stmt, T data) throws Exception {
        throw new RuntimeException("hasDelete时必须实现");
    }

    @Override
    protected void onInit(Configuration parameters) throws Exception {
        initConn();
        stmt = conn.prepareStatement(getUpdateSql());
        LOG.warn("gene_update_sql:" + getUpdateSql());
        if(hasDelete){
            deleteStmt = conn.prepareStatement(getDeleteSql());
            LOG.warn("gene_delete_sql:" + getDeleteSql());
        }
    }

    private void initConn() throws Exception{
        if(StringUtils.isNotBlank(connectionOptions.driverName)){
            Class.forName(connectionOptions.driverName);
        }
        conn = DriverManager.getConnection(connectionOptions.url, connectionOptions.username, connectionOptions.password);
        conn.setAutoCommit(true);
        LOG.info("open conn");
        LOG.info("sql:{}", getUpdateSql());
    }

    @Override
    protected void onFlush(Collection<T> datas) throws Exception {
        if (periodExecSqlStrategy != null) {
            periodExecSql();
        }
        LOG.info("onFlush start");

        final Collection<T> datasSave;
        final Collection<T> datasDelete;
        if(!hasDelete){
            datasSave = datas;
            datasDelete = null;
        }else{
            datasSave = new ArrayList<>(datas.size());
            datasDelete = new ArrayList<>();
            for (T data : datas) {
                if(!isDeleteData(data)){
                    datasSave.add(data);
                }else{
                    datasDelete.add(data);
                }
            }
        }

        if(!datasSave.isEmpty()){
            jdbcOperateExec(() -> saveDatas(datasSave));
        }
        if(hasDelete && !datasDelete.isEmpty()){
            jdbcOperateExec(() -> delDatas(datasDelete));
        }
    }

    private void jdbcOperateExec(Exec exec) throws Exception {
        int i = 0;
        while (i <= maxRetries) {
            try {
                // 成功直接返回
                exec.exec();
                return;
            } catch (SQLException e) {
                if (i >= maxRetries) {
                    throw new IOException(e);
                }

                // 连接超时, 重新连接
                try {
                    if (!conn.isValid(60)) {
                        initConn();
                        if (stmt != null) {
                            stmt.close();
                        }
                        stmt = conn.prepareStatement(getUpdateSql());
                        if(hasDelete){
                            if (deleteStmt != null) {
                                deleteStmt.close();
                            }
                            deleteStmt = conn.prepareStatement(getDeleteSql());
                        }
                    }
                } catch(Exception err) {
                    throw new IOException("Reestablish JDBC connection failed", err);
                }
            }

            i++;
        }
    }

    private void saveDatas(Collection<T> datas) throws Exception {
        for (T data : datas) {
            setStmt(stmt, data);
            stmt.addBatch();
        }
        stmt.executeBatch();
    }

    private void delDatas(Collection<T> datas) throws Exception {
        for (T data : datas) {
            setDeleteStmt(deleteStmt, data);
            deleteStmt.addBatch();
        }
        deleteStmt.executeBatch();
    }

    private void periodExecSql(){
        long ts = System.currentTimeMillis();
        List<String> sqls = periodExecSqlStrategy.sqlsThisTime(ts);
        if(sqls != null){
            try {
                for (String sql : sqls) {
                    int rst = stmt.executeUpdate(sql);
                    LOG.info("executeUpdate sql:{}, rst:{}", sql, rst);
                }
            } catch(Exception e)  {
                LOG.error("periodExecSql error", e);
            }
        }
    }

    @Override
    protected void onClose() throws Exception {
        if (stmt != null) {
            LOG.info("close stmt");
            stmt.close();
        }
        if (deleteStmt != null) {
            LOG.info("close deleteStmt");
            deleteStmt.close();
        }
        if (conn != null) {
            LOG.info("close conn");
            conn.close();
        }
    }

    @FunctionalInterface
    public interface Exec {
        void exec() throws Exception;
    }
}
