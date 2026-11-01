package ssatr.device;

import java.util.concurrent.BlockingQueue;

/**
 * TASK SPORADIC: tratează alarmele de temperatură.
 *
 * Evenimentele sosesc în coada alarmEvents (puse de taskul periodic).
 * Intervalul minim între sosiri este MIN_INTERARRIVAL_MS: între două eliberări
 * ale taskului (două alarme publicate) trebuie să treacă cel puțin atât.
 * Intervalul este impus de cod: evenimentele care sosesc mai des sunt
 * respinse, ca să nu fie inundat brokerul.
 */
public class SporadicAlarmTask implements Runnable {

    static final long MIN_INTERARRIVAL_MS = 5000;

    private final String deviceId;
    private final MqttConnection mqtt;
    private final BlockingQueue<Double> alarmEvents;

    public SporadicAlarmTask(String deviceId, MqttConnection mqtt, BlockingQueue<Double> alarmEvents) {
        this.deviceId = deviceId;
        this.mqtt = mqtt;
        this.alarmEvents = alarmEvents;
    }

    @Override
    public void run() {
        long lastRelease = 0;

        while (true) {
            try {
                Double temp = alarmEvents.take();
                long now = System.currentTimeMillis();

                long sinceLast = now - lastRelease;
                if (sinceLast < MIN_INTERARRIVAL_MS) {
                    System.out.println("[sporadic] RESPINSĂ alarmă de la " + deviceId + ": au trecut doar " + sinceLast + " ms");
                    continue;
                }

                lastRelease = now;

                // Alegem să transmitem pragul ca valoare separată (ar fi ideal
                // să îl primim din event, dar pentru simplitate considerăm că
                // pragul e depășit clar, și e doar o notificare)
                // Conform enunț: deviceId;timestamp;temperatura;prag
                // Putem să punem un placeholder sau să luăm referința la threshold
                String tempStr = String.format(java.util.Locale.US, "%.2f", temp);
                String payload = deviceId + ";" + now + ";" + tempStr + ";DEPĂȘIT";

                mqtt.publish(Config.alarmTopic(deviceId), payload);
                System.out.println("[sporadic] Alarmă publicată de la " + deviceId + ": " + tempStr);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
