package org.lixin.mysql;

import lombok.Data;

/**
 * @author lixin
 * @date 2023/8/17
 */

@Data
public class DataSource {

    private String username = "root";

    private String password = "root";

    private String host = "127.0.0.1";

    private String port = "3306";
    private String databaseName = "information_schema";

    public String getJdbcUrl() {
        return String.format("jdbc:mysql://%s:%s/%s", host, port, databaseName);
    }

    public DataSource getClone() {
        DataSource dataSource = new DataSource();
        dataSource.setUsername(this.username);
        dataSource.setPassword(this.password);
        dataSource.setHost(this.host);
        dataSource.setPort(this.port);
        dataSource.setDatabaseName(this.databaseName);
        return dataSource;
    }
}
