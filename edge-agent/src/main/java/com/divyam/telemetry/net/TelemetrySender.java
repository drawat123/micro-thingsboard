package com.divyam.telemetry.net;

import com.divyam.telemetry.common.domain.SensorReading;
import com.divyam.telemetry.common.net.SensorReadingCodec;
import com.divyam.telemetry.proto.SensorReadingProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns the network connection from the Edge Agent to the backend.
 * <p>
 * Producer-consumer architecture with automatic reconnect:
 * - Callers enqueue readings (non-blocking)
 * - A supervisory virtual thread maintains the connection, reconnecting
 * with exponential backoff + jitter when the link drops
 * - Samples produced during disconnect are buffered in the queue (subject to capacity)
 */
public class TelemetrySender {

    private static final Logger log = LoggerFactory.getLogger(TelemetrySender.class);

    private static final int QUEUE_CAPACITY = 1000;
    private static final long STOP_TIMEOUT_MS = 5_000;

    // Backoff parameters
    private static final long INITIAL_BACKOFF_MS = 1_000;       // 1 second
    private static final long MAX_BACKOFF_MS = 30_000;          // 30 seconds
    private static final int MAX_CONNECT_ATTEMPTS_AT_STARTUP = 5;  // bounded retry at start()
    // After start() succeeds at least once, the supervisor reconnects forever (no max attempts)

    private final String host;
    private final int port;

    private final BlockingQueue<SensorReading> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

    // Two flags now:
    // - 'running': true between start() and stop(). False means "we're shutting down forever."
    // - The supervisor loop checks this to decide whether to keep reconnecting.
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Socket clientSocket;     // volatile: written by supervisor, read by stop()
    private volatile OutputStream outputStream;       // same reasoning

    private Thread supervisorThread;           // owns the connect-drain-reconnect loop

    public TelemetrySender(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Starts the sender. Attempts to establish the first connection with bounded retry.
     * If no connection can be established within {@code MAX_CONNECT_ATTEMPTS_AT_STARTUP},
     * throws IOException — fail-fast at startup.
     *
     * @throws IOException           if no initial connection could be established
     * @throws IllegalStateException if start() has already been called
     */
    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Sender already started");
        }

        if (!tryConnectWithBackoff(MAX_CONNECT_ATTEMPTS_AT_STARTUP)) {
            running.set(false);
            throw new IOException("Unable to start telemetry sender");
        }

        supervisorThread = Thread.ofVirtual().name("telemetry-supervisor").start(this::runSupervisor);
    }

    /**
     * Non-blocking enqueue. Drops if queue is full OR sender is stopped.
     */
    public boolean enqueue(SensorReading reading) {
        if (!running.get()) {
            return false;
        }

        if (!queue.offer(reading)) {
            log.warn("Telemetry queue full (capacity={}); dropping reading for sensor={}", QUEUE_CAPACITY, reading.sensorId());
            return false;
        }

        return true;
    }

    /**
     * Stop the sender permanently. Signals the supervisor to exit,
     * closes the current connection, and waits briefly for the supervisor to finish.
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        log.info("Shutting down Telemetry Sender...");

        closeCurrentConnection();

        if (supervisorThread != null && Thread.currentThread() != supervisorThread) {
            supervisorThread.interrupt();
            try {
                supervisorThread.join(STOP_TIMEOUT_MS);
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for supervisor to stop");
                Thread.currentThread().interrupt();
            }
        }

        log.info("Telemetry Sender stopped successfully.");
    }

    /**
     * The supervisor loop. Lives for the entire start()..stop() lifecycle.
     * Repeatedly: connect (with unbounded backoff) → drain until error → loop back.
     */
    private void runSupervisor() {
        log.info("Supervisor started");

        while (running.get()) {
            if ((clientSocket == null || clientSocket.isClosed()) &&
                    !tryConnectWithBackoff(Integer.MAX_VALUE)) {
                return;
            }

            runDrainLoop();
            closeCurrentConnection();
        }

        log.info("Supervisor exited");
    }

    /**
     * Drains the queue, writing to the socket, until a write fails or running becomes false.
     * Returns when either condition occurs — the caller (supervisor) decides what to do next.
     */
    private void runDrainLoop() {
        log.info("Drain loop started");

        while (running.get()) {
            try {
                SensorReading reading = queue.take();   // blocks
                SensorReadingProto proto = SensorReadingCodec.toProto(reading);
                proto.writeDelimitedTo(outputStream);
                outputStream.flush();
            } catch (IOException e) {
                log.error("Socket write failed; exiting drain loop", e);
                break;
            } catch (InterruptedException e) {
                // Normal shutdown path: stop() interrupted us. Exit cleanly.
                Thread.currentThread().interrupt();
                break;
            }
        }

        log.info("Drain loop exited");
    }

    /**
     * Attempts to connect with exponential backoff + jitter.
     *
     * @param maxAttempts maximum attempts before giving up. Use Integer.MAX_VALUE for unbounded.
     * @return true if connected, false if maxAttempts exceeded or stop() was called
     */
    private boolean tryConnectWithBackoff(int maxAttempts) {
        long backoff = INITIAL_BACKOFF_MS;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            if (!running.get()) {
                return false;   // stop() was called during backoff
            }

            try {
                clientSocket = new Socket(host, port);
                outputStream = clientSocket.getOutputStream();

                log.info("Connected to {}:{} (attempt={})", host, port, attempt);
                return true;
            } catch (IOException e) {
                log.warn("Reconnect failed (attempt={}, backoff_ms={}, cause={})", attempt, backoff, e.getMessage());
            }

            if (attempt < maxAttempts) {
                long jittered = backoff / 2 + ThreadLocalRandom.current().nextLong(backoff); // gives a value in [backoff/2, backoff*1.5)
                try {
                    Thread.sleep(jittered);
                } catch (InterruptedException e) {
                    return false;
                }

                backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
            }
        }

        return false;
    }

    /**
     * Closes the current socket and writer. Safe to call when already closed.
     */
    private void closeCurrentConnection() {
        try {
            if (clientSocket != null && !clientSocket.isClosed()) {
                clientSocket.close();
            }
        } catch (IOException e) {
            log.warn("Socket close failed: {}", e.getMessage());
        }

        outputStream = null;
        clientSocket = null;
    }
}