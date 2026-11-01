package ssatr.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Locale;

public class Dashboard {

    // Structura internă pentru starea unui device
    private static class DeviceState {
        String lastTemp = "-";
        long lastMessageTime = 0;
        int alarmCount = 0;
    }

    private final Map<String, DeviceState> devices = new ConcurrentHashMap<>();

    // Metode de actualizare apelate din MessageProcessor
    public void updateTelemetry(String deviceId, String temp, long timestamp) {
        DeviceState state = devices.computeIfAbsent(deviceId, k -> new DeviceState());
        state.lastTemp = temp;
        state.lastMessageTime = timestamp;
    }

    public void incrementAlarm(String deviceId, long timestamp) {
        DeviceState state = devices.computeIfAbsent(deviceId, k -> new DeviceState());
        state.alarmCount++;
        state.lastMessageTime = timestamp;
    }

    public void updateStatus(String deviceId, long timestamp) {
        DeviceState state = devices.computeIfAbsent(deviceId, k -> new DeviceState());
        state.lastMessageTime = timestamp;
    }

    public void print() {
        System.out.println("DEVICE\t\tTEMP\tULTIMUL MESAJ\tALARME");
        long now = System.currentTimeMillis();

        if (devices.isEmpty()) {
            System.out.println("(Niciun device conectat încă)");
        }

        for (Map.Entry<String, DeviceState> entry : devices.entrySet()) {
            String id = entry.getKey();
            DeviceState s = entry.getValue();
            String age = s.lastMessageTime == 0 ? "-" : ((now - s.lastMessageTime) / 1000) + " s";
            System.out.println(String.format(Locale.US, "%-10s\t%s\t%s\t\t%d", id, s.lastTemp, age, s.alarmCount));
        }
    }
}