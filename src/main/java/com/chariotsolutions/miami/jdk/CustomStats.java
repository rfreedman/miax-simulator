package com.chariotsolutions.miami.jdk;

import java.nio.ByteBuffer;

/**
 * Holds latency statistics
 */
public class CustomStats {
    private String name;
    private Field[] fields;
    private Number[] values;
    private double[] changePerSecond;

    public void initialize(String name, int count) {
        this.name = name;
        fields = new Field[count];
        values = new Number[count];
        changePerSecond = new double[count];
    }

    public void setField(int index, String name, int lengthInBytes) {
        fields[index] = new Field(name, lengthInBytes);
    }

    public void readValue(int index, ByteBuffer buf, double secondsElapsed) {
        switch(fields[index].size) {
            case 1:
                byte b = buf.get();
                if(values[index] != null) changePerSecond[index] = (b-(Byte)values[index])/secondsElapsed;
                values[index] = b;
                break;
            case 2:
                short s = buf.getShort();
                if(values[index] != null) changePerSecond[index] = (s-(Short)values[index])/secondsElapsed;
                values[index] = s;
                break;
            case 4:
                int i = buf.getInt();
                if(values[index] != null) changePerSecond[index] = (i-(Integer)values[index])/secondsElapsed;
                values[index] = i;
                break;
            case 8:
                long l = buf.getLong();
                if(values[index] != null) changePerSecond[index] = (l-(Long)values[index])/secondsElapsed;
                values[index] = l;
                break;
            default:
                throw new IllegalStateException("Invalid custom field length: "+fields[index].size+" bytes");
        }
    }

    public String getName() {
        return name;
    }

    public Number[] getValues() {
        return values;
    }

    public Field[] getFields() {
        return this.fields;
    }
    
    public void print() {
        System.out.println("CUSTOM "+name);
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            System.out.println(field.name+": "+values[i]+" (rate "+changePerSecond[i]+")");
        }
    }

    public static class Field {
        public String name;
        public int size;

        public Field(String name, int size) {
            this.name = name;
            this.size = size;
        }
    }
}
