package com.melihyarikkaya.rnserialport;

import android.annotation.SuppressLint;

import com.facebook.react.bridge.ReadableMap;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class TcpSocketServer extends TcpSocket {
    private ServerSocket serverSocket;
    private TcpReceiverTask.OnDataReceivedListener mReceiverListener;
    private int clientSocketIds;
    private final ExecutorService executorService;
    private final ConcurrentHashMap<Integer, TcpSocket> socketClients;
    private Future<?> listeningTask;
    private volatile boolean cancelled = false;


    public TcpSocketServer(final ConcurrentHashMap<Integer, TcpSocket> socketClients, final TcpReceiverTask.OnDataReceivedListener receiverListener, final Integer id,
                           final ReadableMap options) throws IOException {
        super(id);
        this.executorService = Executors.newFixedThreadPool(1);
        // Get data from options
        int port = options.getInt("port");
        String address = options.getString("host");
        this.socketClients = socketClients;
        clientSocketIds = (1 + getId()) * 1000;
        // Get the addresses
        InetAddress localInetAddress = InetAddress.getByName(address);
        // Create the socket
        serverSocket = new ServerSocket(port, 50, localInetAddress);

        // setReuseAddress
        try {
            boolean reuseAddress = options.getBoolean("reuseAddress");
            serverSocket.setReuseAddress(reuseAddress);
        } catch (Exception e) {
            // Default to true
            serverSocket.setReuseAddress(true);
        }
        mReceiverListener = receiverListener;
        listen();
    }

    public ServerSocket getServerSocket() {
        return serverSocket;
    }

    /**
     * Next ID for a client socket
     *
     * @return The next ID for a client socket
     */
    private int getClientId() {
        return clientSocketIds++;
    }

    private void listen() {
        cancelled = false;
        listeningTask = executorService.submit(() -> {
            try {
                while (!cancelled && !serverSocket.isClosed()) {
                    Socket socket = serverSocket.accept();
                    int clientId = getClientId();
                    TcpSocketClient socketClient = new TcpSocketClient(mReceiverListener, clientId, socket);
                    socketClients.put(clientId, socketClient);
                    mReceiverListener.onConnection(getId(), clientId, socket);
                    socketClient.startListening();
                }
            } catch (IOException e) {
                if (!serverSocket.isClosed()) {
                    mReceiverListener.onError(getId(), e.getMessage());
                }
            }
        });
    }

    public void close() {
        try {
            // Cancel the listening task
            cancelled = true;
            if (listeningTask != null && !listeningTask.isCancelled()) {
                listeningTask.cancel(true);
            }
            executorService.shutdown();

            // close the socket
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
                mReceiverListener.onClose(getId(), null);
                serverSocket = null;
            }
        } catch (IOException e) {
            mReceiverListener.onClose(getId(), e.getMessage());
        }
    }

    public int getListeningPort() {
        return (serverSocket == null) ? -1 : serverSocket.getLocalPort();
    }
}
