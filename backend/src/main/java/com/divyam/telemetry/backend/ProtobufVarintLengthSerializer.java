package com.divyam.telemetry.backend;

import com.google.protobuf.CodedInputStream;
import org.springframework.integration.ip.tcp.serializer.AbstractByteArraySerializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Serializer that understands Protobuf's writeDelimitedTo framing:
 * a varint-encoded length prefix, then exactly that many bytes of payload.
 * <p>
 * Mirrors what {@code SensorReadingProto.parseDelimitedFrom(InputStream)}
 * does internally, but exposes the raw payload bytes so Spring Integration
 * can route them through its pipeline.
 */
public class ProtobufVarintLengthSerializer extends AbstractByteArraySerializer {

    @Override
    public byte[] deserialize(InputStream inputStream) throws IOException {
        // Read the varint length prefix
        int firstByte = inputStream.read();
        if (firstByte == -1) {
            throw new java.io.EOFException("Stream ended");
        }
        int size = CodedInputStream.readRawVarint32(firstByte, inputStream);

        if (size < 0 || size > getMaxMessageSize()) {
            throw new IOException("Invalid message size: " + size);
        }

        // Read exactly 'size' bytes of payload
        byte[] payload = new byte[size];
        int totalRead = 0;
        while (totalRead < size) {
            int read = inputStream.read(payload, totalRead, size - totalRead);
            if (read == -1) {
                throw new java.io.EOFException("Stream ended mid-message");
            }
            totalRead += read;
        }

        return payload;
    }

    @Override
    public void serialize(byte[] bytes, OutputStream outputStream) throws IOException {
        // We don't send from this side; only receive. Throw if called.
        throw new UnsupportedOperationException("This serializer is receive-only");
    }
}