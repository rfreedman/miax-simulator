package com.chariotsolutions.miami.jdk;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Listens for stats messages
 */
public class StatsClient implements Runnable {
    private final static Database db = new Database();
    private int port;

    public StatsClient(int port) {
        this.port = port;
    }

    public static void main(String[] args) throws IOException {
        new Thread(new Runnable() {
            public void run() {
                while(true) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {}
                    AppInstance instance = db.getAppInstance(1041801);
                    synchronized (instance) {
//                        LatencyStats stats = instance.getLatencyStats(0);
//                        if(stats != null) stats.print();
//                        CapacityStats stats = instance.getCapacityStats(3);
//                        if(stats != null) stats.print();
                        CustomStats stats = instance.getCustomStats(4);
                        if(stats != null) stats.print();
                    }
                }
            }
        }).start();
        for(int i=1; i<21; i++) {
            new Thread(new StatsClient(10000+i)).start();
        }
        System.out.println("Threads launched...");
    }

    public void run() {
        try {
            MulticastSocket socket = new MulticastSocket(port);
            InetAddress address = InetAddress.getByName("230.4.5.6");
            socket.joinGroup(address);
            byte[] buf = new byte[4096];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            Map<Integer, ExecutorService> processors = new HashMap<Integer, ExecutorService>();
            long first = 0;
            long last = 0;
            int count = 0;
            while(true) {
                socket.receive(packet);
                long now = System.currentTimeMillis();
                ++count;
                if(first == 0) {
                    first = now;
                    last = now;
                } else {
                    int since = (int)(now-last);
                    int time = (int)(now-first);
                    if(since > 3000) {
                        System.out.println("Port "+port+" packet rate: "+(count*1000/time)+"/s");
                        last=now;
                    }
                }
    //            System.out.println("Received a packet with length " + packet.getLength());
                ByteBuffer data = ByteBuffer.wrap(packet.getData(), 0, packet.getLength());
                data.order(ByteOrder.LITTLE_ENDIAN);
                StatsMessage msg = StatsMessage.readHeader(data);
                msg.setAppInstance(db.getAppInstance(msg.getApplicationId()));
                ExecutorService exec = processors.get(msg.getApplicationId());
                if(exec == null) {
                    exec = Executors.newSingleThreadExecutor();
                    processors.put(msg.getApplicationId(), exec);
                }
                exec.execute(msg);
                packet.setData(new byte[4096]);
                packet.setLength(buf.length);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
