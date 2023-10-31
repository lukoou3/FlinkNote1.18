package com.java.flink.util;

import com.java.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Predicate;

public class Utils {
    static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    /**
     *
     * @param func 函数操作
     * @param shouldRetry func已发特定异常后是否应该继续重试
     * @param quietTries 前quietTries次重试输出DEBUG level日志
     * @param maxTries 最大重试次数
     * @param retryWait 重试时是否延时, 0不延时立即重试
     * @param messageOnRetry 重试时日志信息, 用于显示日志排查错误
     * @return 首次成功发返回结果
     * @param <T>
     * @throws Exception 重试次数用完，或者shouldRetry返回false
     */
    public static <T> T retry(
            final SupplierWithException<T> func,
            final Predicate<Exception> shouldRetry,
            final int quietTries,
            final int maxTries,
            final long retryWait,
            final String messageOnRetry
    ) throws Exception {
        assert maxTries > 0 : "maxTries > 0";
        assert quietTries >= 0 : "quietTries >= 0";

        int nTry = 0;
        final int maxRetries = maxTries - 1;
        while (true) {
            try {
                nTry++;
                return func.get();
            }
            catch (Exception e) {
                if (nTry < maxTries && shouldRetry.test(e)) {
                    final String fullMessage;
                    if (messageOnRetry == null) {
                        fullMessage = String.format("Retrying (%d of %d)", nTry, maxRetries);
                    } else {
                        fullMessage = String.format( "%s, retrying (%d of %d)", messageOnRetry, nTry, maxRetries
                        );
                    }

                    if (nTry <= quietTries) {
                        LOG.debug(fullMessage, e);
                    } else {
                        LOG.warn(fullMessage, e);
                    }

                    if (retryWait > 0L) {
                        Thread.sleep(retryWait);
                    }
                } else {
                    throw e;
                }
            }
        }
    }

    public static <T> T retry(
            final SupplierWithException<T> func,
            final int maxTries
    ) throws Exception {
        return retry(func, maxTries, 0L, null);
    }
    public static <T> T retry(
            final SupplierWithException<T> func,
            final int maxTries,
            final String messageOnRetry
    ) throws Exception {
        return retry(func, maxTries, 0L, messageOnRetry);
    }

    public static <T> T retry(
            final SupplierWithException<T> func,
            final int maxTries,
            final long retryWait,
            final String messageOnRetry
    ) throws Exception {
        return retry(func, 0, maxTries, retryWait, messageOnRetry);
    }

    public static <T> T retry(
            final SupplierWithException<T> func,
            final int quietTries,
            final int maxTries,
            final long retryWait,
            final String messageOnRetry
    ) throws Exception {
        return retry(func, x -> true, quietTries, maxTries, retryWait, messageOnRetry);
    }
}
