package com.example.relayread.test;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class RelayService {
    @Value("mariadb.relay.name")
    private String logName ;

    @Autowired
    private RedisTemplate<Object, Object> redisTemplate;

    private volatile boolean running = true;

    public void startReadingRelayLog(String relayLogDir) {
        Thread thread = new Thread(() -> watchDirectory(relayLogDir));
        thread.start();
    }


    private void watchDirectory(String relayLogDir) {
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            Path path = Paths.get(relayLogDir);
            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);

            while (running) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();
                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }

                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path fileName = ev.context();

                    if (fileName.toString().startsWith(logName)) {
                        readRelayLog(path.resolve(fileName).toString());
                    }
                }
                boolean valid = key.reset();
                if (!valid) {
                    break;
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void readRelayLog(String relayLogFilePath) {
        File relayLogFile = new File(relayLogFilePath);
        EventDeserializer eventDeserializer = new EventDeserializer();

        try (BinaryLogFileReader reader = new BinaryLogFileReader(relayLogFile, eventDeserializer)) {
            for (Event event; (event = reader.readEvent()) != null; ) {
                EventType eventType = event.getHeader().getEventType();
                EventData data = event.getData();

                if (eventType == EventType.QUERY) {
                    QueryEventData queryEventData = (QueryEventData) data;
                    String sql = queryEventData.getSql();
                    if (sql.startsWith("INSERT INTO")) {
                        parseInsertQuery(sql);
                    } else if (sql.startsWith("UPDATE")) {
                        parseUpdateQuery(sql);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void parseInsertQuery(String sql) {
        Pattern pattern = Pattern.compile("\\(([^)]+)\\) VALUES \\(([^)]+)\\)");
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            String columnsPart = matcher.group(1);
            String valuesPart = matcher.group(2);

            String[] columns = columnsPart.split(",\\s*");
            String[] values = valuesPart.split(",\\s*");

            if (columns.length != values.length) {
                System.err.println("Columns and values count do not match");
                return; // or handle this case as needed
            }

            String groupCode = null, resultCode = null, billCode = null, channel = null;

            for (int i = 0; i < columns.length; i++) {
                String column = columns[i].trim().replace("'", "");
                String value = values[i].trim().replace("'", "");

                switch (column.toLowerCase()) {
                    case "group_code":
                        groupCode = value;
                        break;
                    case "result_code":
                        resultCode = value;
                        break;
                    case "bill_code":
                        billCode = value;
                        break;
                    case "channel":
                        channel = value;
                        break;
                }
            }

            if (groupCode != null && resultCode != null && billCode != null && channel != null) {
                String key = "count:" + groupCode + ":" + billCode;
                String hashKey = channel + ":99";

                redisTemplate.opsForHash().increment(key, hashKey, 1);
                System.out.println("INSERT - Redis Update: " + key + " " + hashKey);
            }
        }
    }

    private void parseUpdateQuery(String sql) {
        Pattern pattern = Pattern.compile("SET ([^WHERE]+) WHERE ([^)]+)");
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            String setPart = matcher.group(1);
            String wherePart = matcher.group(2);

            String[] setClauses = setPart.split(",\\s*");
            String[] whereClauses = wherePart.split("\\s*and\\s*");

            String groupCode = null, resultCode = null, billCode = null, channel = null;

            for (String clause : setClauses) {
                String[] parts = clause.split("=");
                String column = parts[0].trim();
                String value = parts[1].trim().replace("'", "");

                switch (column.toLowerCase()) {
                    case "group_code":
                        groupCode = value;
                        break;
                    case "result_code":
                        resultCode = value;
                        break;
                    case "bill_code":
                        billCode = value;
                        break;
                    case "channel":
                        channel = value;
                        break;
                }
            }
            for (String clause : whereClauses) {
                String[] parts = clause.split("=");
                String column = parts[0].trim();
                String value = parts[1].trim().replace("'", "");

                if (column.equalsIgnoreCase("ums_msg_id") || column.equalsIgnoreCase("type")) {
                    continue;
                }
                switch (column.toLowerCase()) {
                    case "group_code":
                        groupCode = value;
                        break;
                    case "bill_code":
                        billCode = value;
                        break;
                }
            }
            if (resultCode != null && !resultCode.equals("99") && groupCode != null && billCode != null && channel != null) {
                String key = "count:" + groupCode + ":" + billCode;
                String hashKey = channel + ":" + resultCode;
                redisTemplate.opsForHash().increment(key, hashKey, 1);
                System.out.println("UPDATE - Redis Update: " + key + " " + hashKey);
            }
        }
    }
}