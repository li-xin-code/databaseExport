package org.lixin.mysql;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.*;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lixin
 * @date 2023/8/17
 */
public class MysqlExport {
    private final AtomicInteger no = new AtomicInteger(1);

    public static void main(String[] args) {
        DataSource dataSource = new DataSource();
        dataSource.setHost("192.168.1.100");
        dataSource.setPassword("123qwe!@#");
        String exportPath = "d:/database_export/";
        MysqlExport export = new MysqlExport();
        export.exportAll(dataSource, exportPath);
    }

    public void exportAll(DataSource dataSource, String exportPath) {
        final ExecutorService taskExecutor = new ThreadPoolExecutor(
                20,
                20,
                3,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder().setNameFormat("-task-pool-%d").build());
        List<String> allDatabaseName = getAllDatabaseName(dataSource);
        CountDownLatch latch = new CountDownLatch(allDatabaseName.size());

        allDatabaseName.stream()
                .map(databaseName -> {
                    DataSource dataSourceClone = dataSource.getClone();
                    dataSourceClone.setDatabaseName(databaseName);
                    return dataSourceClone;
                })
                .forEach(source -> taskExecutor.submit(() -> {
                    doExport(source, exportPath);
                    latch.countDown();
                }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("end");
        taskExecutor.shutdown();
    }

    private void doExport(DataSource dataSource, String path) {
        doExport(dataSource, path, dataSource.getDatabaseName());
    }

    private void doExport(DataSource dataSource, String path, String filename) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        String jdbcUrl = dataSource.getJdbcUrl();
        String username = dataSource.getUsername();
        String password = dataSource.getPassword();
        String outputFilePath = path + filename + ".sql";
        File directory = new File(path);
        if (!directory.exists()) {
            if (directory.mkdirs()) {
                System.out.println("初始化路径:" + directory.getPath());
            }
        }
        File file = new File(outputFilePath);
        if (file.exists()) {
            File renameFile = new File(path + filename + "_" + no.getAndIncrement() + ".sql");
            if (renameFile.delete()) {
                System.out.println("删除文件:" + renameFile.getPath());
            }
            if (file.renameTo(renameFile)) {
                System.out.println("重命名文件:" + directory.getPath() + " to " + renameFile.getPath());
            }
        }

        String databaseName = dataSource.getDatabaseName();
        // 导出数据到SQL文件
        String commandTemplate = "mysqldump --user=%s --password=%s --host=%s --port=%s %s";
        String exportCommand = String.format(commandTemplate,
                username, password, dataSource.getHost(), dataSource.getPort(),
                databaseName);
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            try {
                Process process = Runtime.getRuntime().exec(exportCommand);

                // 使用BufferedReader读取进程的输出流
                BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                // 创建输出文件
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(file));) {

                    String outLine;
                    // 逐行读取输出并写入文件
                    while ((outLine = reader.readLine()) != null) {
                        writer.write(outLine);
                        writer.newLine();
                    }
                }

                int exitCode = process.waitFor();
                if (exitCode == 0) {
                    System.out.println("Database " + databaseName + " exported successfully");
                } else {
                    System.out.println("Database " + databaseName + " export failed");
                    // 使用BufferedReader读取进程的错误流
                    BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                    String errorLine;

                    // 逐行读取错误输出并打印
                    while ((errorLine = errorReader.readLine()) != null) {
                        System.err.println(errorLine);
                    }
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        stopwatch.stop();
        Duration elapsed = stopwatch.elapsed();
        System.out.println(databaseName + " spend:" + elapsed.toMillis());
    }

    public static List<String> getAllDatabaseName(DataSource dataSource) {
        String jdbcUrl = dataSource.getJdbcUrl();
        String username = dataSource.getUsername();
        String password = dataSource.getPassword();
        List<String> list = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            // ignore database information_schema,mysql,performance_schema,sys
            String sql = "select SCHEMA_NAME " +
                    "from information_schema.SCHEMATA" +
                    " where SCHEMA_NAME not in ('information_schema','mysql','performance_schema','sys');";
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();
            // 遍历结果集并处理数据
            while (resultSet.next()) {
                String name = resultSet.getString("SCHEMA_NAME");
                list.add(name);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }
}
