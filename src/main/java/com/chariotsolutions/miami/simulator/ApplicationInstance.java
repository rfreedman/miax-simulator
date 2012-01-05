package com.chariotsolutions.miami.simulator;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;

/**
 * Simulates an application within a cloud
 */
public class ApplicationInstance {
    private int applicationID;
    private MulticastSocket socket;
    private ConfigPacket configPacket;
    private DataPacket dataPacket;
    private RandomGenerator random;
    private DatagramPacket datagram;
    private short sequenceNumber = 0;
    private int multicastPort;

    public ApplicationInstance(int applicationID, int multicastPort, RandomGenerator random) throws IOException {
        this.applicationID = applicationID;
        this.multicastPort = multicastPort;
        this.random = random;
        InetAddress address = InetAddress.getByName("230.4.5.6");
        socket = new MulticastSocket(multicastPort);
        datagram = new DatagramPacket(new byte[0], 0, address, multicastPort);
        configPacket = new ConfigPacket(applicationID);
        dataPacket = new DataPacket(applicationID);
    }

    public void sendConfigPacket() throws IOException {
        byte[] buf = new byte[configPacket.getSize()];
        configPacket.prepare(sequenceNumber++);
        configPacket.getBuffer().rewind();
        configPacket.getBuffer().get(buf);
        datagram.setData(buf);
        datagram.setLength(buf.length);
        socket.send(datagram);
    }

    public void sendDataPacket() throws IOException {
        byte[] buf;
        if(dataPacket.getSize() == datagram.getLength()) {
            buf = datagram.getData();
        } else {
            buf = new byte[dataPacket.getSize()];
            datagram.setData(buf);
            datagram.setLength(buf.length);
        }
        dataPacket.prepare(sequenceNumber++);

        dataPacket.addLatencyData(0, new long[]{random.getRandomInt(10000), random.getRandomInt(10000), random.getRandomInt(10000),
                random.getRandomInt(10000), random.getRandomInt(10000), random.getRandomInt(10000),
                random.getRandomInt(1000), random.getRandomInt(1000), random.getRandomInt(1000), random.getRandomInt(1000),
                random.getRandomInt(1000), random.getRandomInt(1000), random.getRandomInt(1000), random.getRandomInt(1000)});
        dataPacket.addLatencyData(1, new long[]{random.getRandomInt(10000), random.getRandomInt(10000), random.getRandomInt(10000),
                random.getRandomInt(10000), random.getRandomInt(10000), random.getRandomInt(10000),
                random.getRandomInt(1000), random.getRandomInt(1000), random.getRandomInt(1000), random.getRandomInt(1000),
                random.getRandomInt(1000), random.getRandomInt(1000), random.getRandomInt(1000), random.getRandomInt(1000)});
        dataPacket.addLatencyData(2, new long[]{random.getRandomInt(10000), random.getRandomInt(10000), random.getRandomInt(10000),
                random.getRandomInt(10000), random.getRandomInt(10000), random.getRandomInt(10000),
                random.getRandomInt(1000), random.getRandomInt(1000), random.getRandomInt(1000), random.getRandomInt(1000),
                random.getRandomInt(1000), random.getRandomInt(1000), random.getRandomInt(1000), random.getRandomInt(1000)});
        dataPacket.setCapacityData(new int[]{
                random.getRandomInt(10), random.getRandomInt(20), random.getRandomInt(30), random.getRandomInt(40),
                random.getRandomInt(50), random.getRandomInt(60), random.getRandomInt(70), random.getRandomInt(80),
                random.getRandomInt(90), random.getRandomInt(80), random.getRandomInt(70), random.getRandomInt(60),
                random.getRandomInt(50), random.getRandomInt(40), random.getRandomInt(30), random.getRandomInt(20),
                random.getRandomInt(10), random.getRandomInt(10), random.getRandomInt(10), random.getRandomInt(10)
        });
        dataPacket.addCustom1(random.getRandomInt(100), random.getRandomInt(100), random.getRandomInt(100),
                random.getRandomShort(100), random.getRandomShort(100), random.getRandomShort(100), random.getRandomShort(100),
                random.getRandomShort(100), random.getRandomShort(100), random.getRandomShort(100),
                random.getRandomShort(100), random.getRandomShort(100), random.getRandomShort(100), random.getRandomShort(100),
                random.getRandomShort(100), random.getRandomShort(100), random.getRandomShort(100), random.getRandomShort(100),
                random.getRandomInt(100), random.getRandomInt(100), random.getRandomInt(100),
                random.getRandomByte(100), random.getRandomByte(100));
        dataPacket.addCustom2(random.getRandomInt(100), random.getRandomInt(1000000),
                random.getRandomInt(100), random.getRandomInt(1000000),
                random.getRandomInt(100), random.getRandomInt(100), random.getRandomInt(1000000));

        dataPacket.getBuffer().rewind();
        dataPacket.getBuffer().get(buf);
        socket.send(datagram);
//        System.out.println("Sent to port "+multicastPort);
    }
}
