package com.chariotsolutions.miami.simulator;

import java.io.IOException;
import java.util.*;

/**
 * Like the original Cloud class,
 * but only generates stats for one application type
 * (on one port)
 */
public class SingleAppCloud implements Runnable {
    private static final int BROADCAST_PORT = 10000;
    private final Random rand;
    private final ApplicationInstance[] apps;

    private static final int interval = 2000; // number of msec. between sending packets
    private static final int numClouds = 24;
    private static final int appsPerCloud = 64;

    private int index;

    public int getIndex() {
        return index;
    }

    public int getAppsCount() {
       return apps.length;
    }

    public SingleAppCloud(int index, int port) throws IOException {
        this.index = index;
        rand = new Random(index);
        apps = new ApplicationInstance[appsPerCloud];
       // Map<Integer, Integer> ports = new HashMap<Integer, Integer>();
        for (int i = 0; i < apps.length; i++) {
            int appType = i; //= rand.nextInt(20)+1;
            int firm = 6; //rand.nextInt(5)+1;
            /*
            Integer lastPort = ports.get(appType*100+firm);
            int port;
            if(lastPort == null) {
                port = 1;
            } else {
                port = lastPort+1;
            }
            ports.put(appType*100+firm, port);
            */
            int appID = index*1000000+firm*10000+appType*100+port;
            int netPort = port; //10000+appType;

            System.out.println("App Instance "+i+": cloud "+index+" firm "+firm+" app "+appType+" port "+port+" app ID "+appID+" network port "+netPort);
            apps[i] = new ApplicationInstance(appID, appType, netPort, new RandomGenerator() {
                public int getRandomInt(int max) {
                    synchronized(rand) {
                        return rand.nextInt(max);
                    }
                }

                public short getRandomShort(int max) {
                    synchronized(rand) {
                        if(max > Short.MAX_VALUE) throw new IllegalArgumentException();
                        return (short)rand.nextInt(max);
                    }
                }

                public byte getRandomByte(int max) {
                    synchronized(rand) {
                        if(max > Byte.MAX_VALUE) throw new IllegalArgumentException();
                        return (byte)rand.nextInt(max);
                    }
                }
            });
        }
    }

    public void run() {
        final int numberOfLoops =  999999999;
        final int delay = interval / apps.length;
        System.out.println("cloud: " + this.getIndex() + " sending a data packet every " + delay + " msec.");
        try {

            for (ApplicationInstance app : apps) {
                app.sendConfigPacket();
                Thread.sleep(800);
            }

            apps[0].sendConfigPacket();

            for(int i=0; i<numberOfLoops; i++) {
                long before = new Date().getTime();
                for (ApplicationInstance app : apps) {
                    app.sendDataPacket();
                    //Thread.sleep(1000);
                    Thread.sleep(delay);
                }
                //Thread.sleep(interval);
                long after = new Date().getTime();
                long duration = after - before;
                System.out.println("cloud: " + this.getIndex() + " sent " + apps.length + " packets in " + duration + " msec.");
                if(duration < interval) {
                    Thread.sleep(interval - duration);
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        List<SingleAppCloud> clouds = new ArrayList<SingleAppCloud>(numClouds);
        for(int i=0; i< numClouds; i++) {
            //clouds.add(new Cloud(i+1));
             clouds.add(new SingleAppCloud(i+1, BROADCAST_PORT));
        }

        int totalApps = 0;
        for(SingleAppCloud cloud : clouds) {
            totalApps += cloud.getAppsCount();
            System.out.println("cloud: " + cloud.getIndex() + ": " + cloud.getAppsCount() + " apps");
        }
        System.out.println("total apps: " + totalApps);
        int startupDelay = interval / clouds.size();

        for (SingleAppCloud cloud : clouds) {
            new Thread(cloud).start();
            Thread.sleep(startupDelay);
        }
    }
}

