package ssatr.service;

import java.util.concurrent.BlockingQueue;

/**
 * Preia mesajele de un anumit tip ("telemetry", "alarm" sau "status") din coada
 * inbox, le salvează și actualizează dashboard-ul.
 *
 * Formatul mesajelor (câmpuri separate prin ";"):
 *   telemetry:  deviceId;timestamp;temperatura
 *   alarm:      deviceId;timestamp;temperatura;prag
 *   status:     deviceId;timestamp;text
 */
public class MessageProcessor implements Runnable {

    private final String kind;
    private final BlockingQueue<String> inbox;
    private final CsvStorage storage;
    private final Dashboard dashboard;

    public MessageProcessor(String kind, BlockingQueue<String> inbox, CsvStorage storage, Dashboard dashboard) {
        this.kind = kind;
        this.inbox = inbox;
        this.storage = storage;
        this.dashboard = dashboard;
    }

    @Override
    public void run() {
        while (true) {
            try {
                String message = inbox.take();
                storage.append(kind, message);

                String[] parts = message.split(";");
                if (parts.length < 3) continue; // Format invalid, ignorăm mesajul

                String deviceId = parts[0];
                long timestamp = Long.parseLong(parts[1]);

                if (kind.equals("telemetry")) {
                    dashboard.updateTelemetry(deviceId, parts[2], timestamp);
                } else if (kind.equals("alarm")) {
                    dashboard.incrementAlarm(deviceId, timestamp);
                    System.out.println("[ALARMĂ] de la " + deviceId + ": Temperatura=" + parts[2] + ", Prag=" + parts[3]);
                } else if (kind.equals("status")) {
                    dashboard.updateStatus(deviceId, timestamp);
                    System.out.println("[STATUS] Confirmare de la " + deviceId + ": " + parts[2]);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.out.println("Eroare la procesarea mesajului: " + e.getMessage());
            }
        }
    }
}
