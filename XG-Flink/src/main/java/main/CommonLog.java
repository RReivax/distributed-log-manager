package main;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

// Class used to transform incoming Stings to formatted logs
public class CommonLog {

    public CommonLog() {}

    public CommonLog(String ip, String userIdentifier, String userId, ZonedDateTime time, String request, int httpStatus,
                     int objectSize) {
        this.ip = ip;
        this.userIdentifier = userIdentifier;
        this.userId = userId;
        this.time = time;
        this.request = request;
        this.httpStatus = httpStatus;
        this.objectSize = objectSize;
    }

    public String ip;
    public String userIdentifier;
    public String userId;
    public ZonedDateTime time;
    public String request;
    public int httpStatus;
    public int objectSize;

    public String toString() {
        DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        StringBuilder sb = new StringBuilder();
        sb.append(ip).append(" ");
        sb.append(userIdentifier).append(" ");
        sb.append(userId).append(" ");
        sb.append("[").append(time.format(f)).append("] ");
        sb.append("\"").append(request).append("\" ");
        sb.append(httpStatus == -1 ? "-" : httpStatus).append(" ");
        sb.append(objectSize == -1 ? "-" : objectSize);

        return sb.toString();
    }

    public static CommonLog fromString(String log) {
        DateTimeFormatter f = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss ZZ");

        String[] tokens = log.split("\"");
        String[] part1 = tokens[0].split(" ");
        String req = tokens[1];
        String[] part3 = tokens[2].split(" ");
        String date = (part1[3] + " " + part1[4]).split("\\[")[1].split("]")[0];

        CommonLog comLog = new CommonLog();

        try {
            comLog.ip = part1[0];
            comLog.userIdentifier = part1[1];
            comLog.userId = part1[2];
            comLog.time = ZonedDateTime.parse(date, f);
            comLog.request = req;

            switch(part3[1]){
                case "-":
                    comLog.httpStatus = -1;
                    break;
                default:
                    comLog.httpStatus = Integer.parseInt(part3[1]);
                    break;
            }

            switch (part3[2]){
                case "-":
                    comLog.objectSize = -1;
                    break;
                default:
                    comLog.objectSize = Integer.parseInt(part3[2]);
                    break;
            }
        } catch (Exception e) {
            throw new RuntimeException("Error: " + e);
        }

        return comLog;
    }
}
