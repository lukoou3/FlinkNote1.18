package com.java.flink.connector.clickhouse.buffer;

import com.github.housepower.data.ColumnWriterBuffer;
import com.github.housepower.serde.BinarySerializer;
import com.github.housepower.settings.ClickHouseDefines;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.List;

import static java.lang.Math.min;

public class ReusedColumnWriterBuffer extends ColumnWriterBuffer {
    private static Field columnWriterField;
    private final ReusedByteArrayWriter reusedColumnWriter;

    public ReusedColumnWriterBuffer(BufferPool bufferPool) {
        super();
        try {
            columnWriterField.set(this, null);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        this.reusedColumnWriter = new ReusedByteArrayWriter(ClickHouseDefines.COLUMN_BUFFER_BYTES, bufferPool);
        this.column = new BinarySerializer(reusedColumnWriter, false);
    }

    static {
        try {
            columnWriterField = ColumnWriterBuffer.class.getDeclaredField("columnWriter");
            columnWriterField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void writeTo(BinarySerializer serializer) throws IOException {
        // add a temp buffer to reduce memory requirement in case of large remaining data in buffer
        byte[] writeBuffer = new byte[4*1024];
        for (ByteBuffer buffer : reusedColumnWriter.getBufferList()) {
            // upcast is necessary, see detail at:
            // https://bitbucket.org/ijabz/jaudiotagger/issues/313/java-8-javalangnosuchmethoderror
            ((Buffer) buffer).flip();
            while (buffer.hasRemaining()) {
                int remaining = buffer.remaining();
                int thisLength = min(remaining, writeBuffer.length);
                buffer.get(writeBuffer, 0, thisLength);
                serializer.writeBytes(writeBuffer, 0, thisLength);
            }
        }
    }

    @Override
    public void reset() {
        reusedColumnWriter.reset();
    }

    public List<ByteBuffer> getBufferList() {
        return reusedColumnWriter.getBufferList();
    }
}
