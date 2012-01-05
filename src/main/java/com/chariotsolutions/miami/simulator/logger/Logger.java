package com.chariotsolutions.miami.simulator.logger;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

/**
 * Just a test
 */
public class Logger {
    public static void main(String[] args) throws IOException {
        MulticastSocket socket = new MulticastSocket(10006);
        InetAddress address = InetAddress.getByName("230.4.5.6");
        socket.joinGroup(address);
        byte[] buf = new byte[4096];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        int index = 0;
        while(true) {
            socket.receive(packet);
            System.out.println("Received a packet with length "+packet.getLength());
            FileOutputStream out = new FileOutputStream(packet.getLength()>500?"config_packet.bin":"data_packet_"+index+".bin");
            ++index;
            out.write(packet.getData(), 0, packet.getLength());
            out.flush();
            out.close();
            packet.setLength(buf.length);
        }
    }
}
