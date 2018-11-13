package com.bobocode;

import com.bobocode.util.FileReader;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * {@link UserProfileDbInitializer} is an API that has only one method. It allow to create a database tables to store
 * information about users and their profiles.
 */
class UserProfileDbInitializer {
    private final static String TABLE_INITIALIZATION_SQL_FILE = "db/migration/table_initialization.sql";
    private DataSource dataSource;

    UserProfileDbInitializer(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Reads the SQL script form the file and executes it
     *
     * @throws SQLException
     */
    void init() throws SQLException {
        String createTablesSql = FileReader.readWholeFileFromResources(TABLE_INITIALIZATION_SQL_FILE);

        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(createTablesSql);
        }
    }

}
