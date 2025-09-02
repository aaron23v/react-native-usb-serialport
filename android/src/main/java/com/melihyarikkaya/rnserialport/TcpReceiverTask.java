package com.melihyarikkaya.rnserialport;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * This is a specialized task that receives data from a socket in the background, and
 * notifies it's listener when data is received. This is not threadsafe, the listener
 * should handle synchronicity.
 * 
 * Updated to use modern ExecutorService instead of deprecated AsyncTask for React Native 0.80 compatibility
 */
class TcpReceiverTask {
    private static final ExecutorService executorService = Executors.newCachedThreadPool();
    private Future<?> currentTask;
    private volatile boolean cancelled = false;
    /**
     * Execute the task with modern ExecutorService instead of deprecated AsyncTask
     */
    public void executeOnExecutor(TcpSocketClient clientSocket, OnDataReceivedListener receiverListener) {
        cancelled = false;
        currentTask = executorService.submit(() -> {
            int socketId = clientSocket.getId();
            Socket socket = clientSocket.getSocket();
            byte[] buffer = new byte[8192];
            int bufferCount;
            try {
                BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
                while (!cancelled && !socket.isClosed()) {
                    bufferCount = in.read(buffer);
                    if (bufferCount > 0) {
                        receiverListener.onData(socketId, Arrays.copyOfRange(buffer, 0, bufferCount));
                    } else if (bufferCount == -1) {
                        clientSocket.destroy();
                        break;
                    }
                }
            } catch (IOException ioe) {
                if (receiverListener != null && !socket.isClosed()) {
                    receiverListener.onError(socketId, ioe.getMessage());
                }
                cancel(false);
            }
        });
    }

    /**
     * Cancel the current task execution
     */
    public boolean cancel(boolean mayInterruptIfRunning) {
        cancelled = true;
        if (currentTask != null) {
            return currentTask.cancel(mayInterruptIfRunning);
        }
        return true;
    }

    /**
     * Check if task is cancelled
     */
    public boolean isCancelled() {
        return cancelled;
    }

    /**
     * Listener interface for receive events.
     */
    @SuppressWarnings("WeakerAccess")
    public interface OnDataReceivedListener {
        void onConnection(Integer serverId, Integer clientId, Socket socket);

        void onConnect(Integer id, TcpSocketClient client);

        void onListen(Integer id, TcpSocketServer server);

        void onData(Integer id, byte[] data);

        void onClose(Integer id, String error);

        void onError(Integer id, String error);
    }
}
