package com.chariotsolutions.miami.simulator;

/**
 * Provides "random" data
 */
public interface RandomGenerator {
    int getRandomInt(int max);
    short getRandomShort(int max);
    byte getRandomByte(int max);
}
