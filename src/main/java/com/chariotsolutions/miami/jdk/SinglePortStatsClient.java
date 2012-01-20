package com.chariotsolutions.miami.jdk;

import org.java.util.concurrent.NotifyingBlockingThreadPoolExecutor;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Listens for stats messages
 */
public class SinglePortStatsClient implements Runnable {
    private static final String BROADCAST_ADDRESS = "230.4.5.6"; // "239.37.11.63";
    private static final int BROADCAST_PORT = 10000; //30371;
    private static final int PACKET_SIZE = 4096;

    private static final Database db = new Database();
    private String broadcastAddress;
    private int port;

    private int messagesReceived;


    private static ThreadPoolExecutor threadPool;

    static {
        initThreadPool();
    }


    public SinglePortStatsClient(String broadcastAddress, int port) {
        this.broadcastAddress = broadcastAddress;
        this.port = port;
    }

    public static void main(String[] args) throws IOException {
        new Thread(new SinglePortStatsClient(BROADCAST_ADDRESS, BROADCAST_PORT)).start();
        System.out.println("Single Threaded sequential packet processor started...");
    }

    public /*synchronized*/ void incrementMessagesReceived() {
        messagesReceived += 1;
    }

    public /*synchronized*/ int getMessagesReceived() {
        return this.messagesReceived;
    }

    public void run() {
        try {
            MulticastSocket socket = new MulticastSocket(port);
            InetAddress address = InetAddress.getByName(broadcastAddress);
            socket.joinGroup(address);
            DatagramPacket packet = new DatagramPacket(new byte[PACKET_SIZE], PACKET_SIZE);

            while(true) {
                socket.receive(packet);


                ByteBuffer data = ByteBuffer.wrap(packet.getData(), 0, packet.getLength());
                data.order(ByteOrder.LITTLE_ENDIAN);
                StatsMessage msg = StatsMessage.readHeader(data);
                msg.setAppInstance(db.getAppInstance(msg.getApplicationId()));
                threadPool.execute(msg);

                packet.setData(new byte[PACKET_SIZE]);
                packet.setLength(PACKET_SIZE);

                incrementMessagesReceived();
                int rcvd = getMessagesReceived();
                System.out.println("messages received: " + rcvd);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static void initThreadPool() {
        int poolSize = 200; // 64 ports, 2 stat packets each - should be no waiting
        int queueSize = poolSize * 2; // recommended queue size
        int threadKeepAliveTime = 50;
        TimeUnit threadKeepAliveTimeUnit = TimeUnit.MILLISECONDS;
        int maxBlockingTime = 100;
        TimeUnit maxBlockingTimeUnit = TimeUnit.MILLISECONDS;

        Callable<Boolean> blockingTimeoutCallback = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                System.out.println("waiting for task insertion...");
                return true;
            }
        };


        threadPool = new NotifyingBlockingThreadPoolExecutor(
                poolSize,
                queueSize,
                threadKeepAliveTime, threadKeepAliveTimeUnit,
                maxBlockingTime, maxBlockingTimeUnit,
                blockingTimeoutCallback
        );
    }


}
