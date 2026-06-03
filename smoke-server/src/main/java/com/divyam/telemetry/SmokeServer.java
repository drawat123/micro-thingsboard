package com.divyam.telemetry;

import com.divyam.telemetry.common.domain.SensorReading;
import com.divyam.telemetry.common.net.SensorReadingCodec;
import com.divyam.telemetry.proto.SensorReadingProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;

/**
 * A minimal, multi-client TCP listener.
 * The acceptor thread does nothing but accept;
 * each connection is handled on its own virtual thread, so thousands of clients can connect simultaneously.
 * <p>
 * This is a smoke-test harness, NOT a production server.
 */
public class SmokeServer {

    private static final Logger log = LoggerFactory.getLogger(SmokeServer.class);

    private static final int PORT = 5555;

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PORT);
             var executor =
                     Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("client-", 0).factory())) {
            log.info("Server started listening on PORT: {}", PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept(); // main thread blocks here
                executor.submit(() -> handleClient(clientSocket)); // virtual thread takes it
            }
        } catch (IOException e) {
            log.warn("I/O error while communicating with client", e);
        } catch (IllegalArgumentException e) {
            log.error("Received invalid sensor reading; closing client", e);
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (clientSocket;
             InputStream in = clientSocket.getInputStream()) {
            log.info("Client connected: {}", clientSocket.getRemoteSocketAddress());

            SensorReadingProto proto;
            while ((proto = SensorReadingProto.parseDelimitedFrom(in)) != null) {
                SensorReading reading = SensorReadingCodec.fromProto(proto);
                log.info("Received from {}: {}", clientSocket.getRemoteSocketAddress(), reading);
            }

            log.info("Client disconnected gracefully: {}", clientSocket.getRemoteSocketAddress());
        } catch (IOException e) {
            log.warn("I/O error while communicating with client {}", clientSocket.getRemoteSocketAddress(), e);
        }
    }
}