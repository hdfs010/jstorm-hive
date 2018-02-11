package com.alibaba.jstorm.hive.common;

import java.util.HashMap;

public class Config  extends HashMap<String, Object> {
    public static final String TOPOLOGY_AUTO_CREDENTIALS = "topology.auto-credentials";
    public static final String TOPOLOGY_TICK_TUPLE_FREQ_SECS="topology.tick.tuple.freq.secs";
}
