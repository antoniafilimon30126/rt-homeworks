package ssatr.device;

import java.util.concurrent.BlockingQueue;

/**
 * TASK PERIODIC, cu perioada T = periodMs: fiecare job citește senzorul și
 * publică valoarea pe topic-ul de telemetrie.
 *
 * Dacă temperatura depășește pragul, taskul NU tratează el alarma, ci doar
 * semnalează un eveniment în coada alarmEvents. Alarma este tratată de
 * taskul sporadic.
 */
public class PeriodicTelemetryTask implements Runnable {

    private final String deviceId;
    private final TemperatureSensor sensor;
    private final MqttConnection mqtt;
    private final BlockingQueue<Double> alarmEvents;

    // volatile: sunt citite aici, dar modificate de taskul aperiodic (alt thread)
    private volatile long periodMs = 2000;
    private volatile double threshold = 28.0;

    public PeriodicTelemetryTask(String deviceId, TemperatureSensor sensor, MqttConnection mqtt,
                                 BlockingQueue<Double> alarmEvents) {
        this.deviceId = deviceId;
        this.sensor = sensor;
        this.mqtt = mqtt;
        this.alarmEvents = alarmEvents;
    }

    @Override
    public void run() {
        long nextRelease = System.nanoTime(); // Prima eliberare este acum

        while (true) {
            try {
                // Setează eliberarea pe grilă (r_k = r_{k-1} + T)
                nextRelease += periodMs * 1_000_000L;

                long currentNano = System.nanoTime();
                long sleepTimeMs = (nextRelease - currentNano) / 1_000_000L;

                if (sleepTimeMs > 0) {
                    Thread.sleep(sleepTimeMs); // Așteptare absolută
                }

                // Citim și publicăm
                double temp = sensor.read();
                long timestamp = System.currentTimeMillis();
                String tempStr = String.format(java.util.Locale.US, "%.2f", temp);

                String payload = deviceId + ";" + timestamp + ";" + tempStr;
                mqtt.publish(Config.telemetryTopic(deviceId), payload);

                // Dacă depășește pragul, semnalăm alarmă
                if (temp > threshold) {
                    alarmEvents.put(temp);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void setPeriodMs(long periodMs) {
        this.periodMs = periodMs;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public long getPeriodMs() {
        return periodMs;
    }

    public double getThreshold() {
        return threshold;
    }
}
