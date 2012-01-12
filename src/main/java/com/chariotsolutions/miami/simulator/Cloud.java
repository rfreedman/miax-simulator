package com.chariotsolutions.miami.simulator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Simulates a cloud
 */
public class Cloud implements Runnable {
    private final Random rand;
    private final ApplicationInstance[] apps;

    private static final int interval = 2000; // number of msec. between sending packets
    private static final int numClouds = 24; // 24
    private static final int appsPerCloud = 64; //32;


    private Integer index;

    public Integer getIndex() {
        return index;
    }

    public int getAppsCount() {
       return apps.length;
    }


    public Cloud(int index) throws IOException {
        this.index = index;
        rand = new Random(index);
        apps = new ApplicationInstance[appsPerCloud];
        Map<Integer, Integer> ports = new HashMap<Integer, Integer>();
        for (int i = 0; i < apps.length; i++) {
            int appType = i; //= rand.nextInt(20)+1;
            int firm = rand.nextInt(5)+1;
            Integer lastPort = ports.get(appType*100+firm);
            int port;
            if(lastPort == null) {
                port = 1;
            } else {
                port = lastPort+1;
            }
            ports.put(appType*100+firm, port);
            int appID = index*1000000+firm*10000+appType*100+port;
            int netPort = 10000+appType;

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
        try {
            for (ApplicationInstance app : apps) {
                app.sendConfigPacket();
            }
            for(int i=0; i<numberOfLoops; i++) {
                for (ApplicationInstance app : apps) {
                    app.sendDataPacket();
                    Thread.sleep(50);
                }
                Thread.sleep(interval);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        List<Cloud> clouds = new ArrayList<Cloud>(numClouds);
        for(int i=0; i< numClouds; i++) {
            clouds.add(new Cloud(i+1));
        }

        int totalApps = 0;
        for(Cloud cloud : clouds) {
            totalApps += cloud.getAppsCount();
            System.out.println("cloud: " + cloud.getIndex() + ": " + cloud.getAppsCount() + " apps");
        }
        System.out.println("total apps: " + totalApps);
        for (Cloud cloud : clouds) {
            new Thread(cloud).start();
        }
    }
}
