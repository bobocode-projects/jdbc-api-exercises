package com.bobocode;

import com.bobocode.util.JdbcUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


public class UserProfileDbInitializerTest {
    private static DataSource dataSource;

    @BeforeClass
    public static void init() throws SQLException {
        dataSource = JdbcUtil.createDefaultInMemoryH2DataSource();
        UserProfileDbInitializer dbInitializer = new UserProfileDbInitializer(dataSource);
        dbInitializer.init();
    }

    @Test
    public void testTablesHaveCorrectNames() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();

            ResultSet resultSet = statement.executeQuery("SHOW TABLES");
            List<String> tableNames = fetchTableNames(resultSet);

            assertThat(tableNames, containsInAnyOrder("users", "profiles"));
        }
    }

    private List<String> fetchTableNames(ResultSet resultSet) throws SQLException {
        List<String> tableNamesList = new ArrayList<>();
        while (resultSet.next()) {
            String tableName = resultSet.getString("table_name");
            tableNamesList.add(tableName);
        }
        return tableNamesList;
    }


    @Test
    public void testUsersTablesHasPrimaryKey() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'users' AND constraint_type = 'PRIMARY_KEY';");

            boolean resultIsNotEmpty = resultSet.next();

            assertThat(resultIsNotEmpty, is(true));
        }
    }

    @Test
    public void testUsersTablePrimaryKeyHasCorrectName() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'users' AND constraint_type = 'PRIMARY_KEY';");

            resultSet.next();
            String pkConstraintName = resultSet.getString("constraint_name");

            assertThat(pkConstraintName, equalTo("users_PK"));
        }
    }

    @Test
    public void testUsersTablePrimaryKeyBasedOnIdField() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'users' AND constraint_type = 'PRIMARY_KEY';");

            resultSet.next();
            String pkColumn = resultSet.getString("column_list");

            assertThat("id", equalTo(pkColumn));
        }
    }

    @Test
    public void testUsersTableHasCorrectAlternativeKey() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'users' AND constraint_type = 'UNIQUE';");

            resultSet.next();
            String uniqueConstraintName = resultSet.getString("constraint_name");
            String uniqueConstraintColumn = resultSet.getString("column_list");

            assertThat(uniqueConstraintName, equalTo("users_email_AK"));
            assertThat(uniqueConstraintColumn, equalTo("email"));
        }
    }

    @Test
    public void testUsersTableHasAllRequiredColumns() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'users';");

            List<String> columns = fetchColumnValues(resultSet, "column_name");

            assertThat(columns.size(), equalTo(5));
            assertThat(columns, containsInAnyOrder("id", "email", "first_name", "last_name", "birthday"));
        }
    }

    private List<String> fetchColumnValues(ResultSet resultSet, String resultColumnName) throws SQLException {
        List<String> columns = new ArrayList<>();
        while (resultSet.next()) {
            String columnName = resultSet.getString(resultColumnName);
            columns.add(columnName);
        }
        return columns;
    }

    @Test
    public void testUsersTableRequiredColumnsHaveHaveNotNullConstraint() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'users' AND nullable = false;");

            List<String> notNullColumns = fetchColumnValues(resultSet, "column_name");

            assertThat(notNullColumns.size(), is(5));
            assertThat(notNullColumns, containsInAnyOrder("id", "email", "first_name", "last_name", "birthday"));
        }
    }

    @Test
    public void testUserIdTypeIsBigint() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'users' AND column_name = 'id';");

            resultSet.next();
            String idTypeName = resultSet.getString("type_name");

            assertThat(idTypeName, is("BIGINT"));
        }
    }

    @Test
    public void testUsersTableStringColumnsHaveCorrectTypeAndLength() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'users' AND type_name = 'VARCHAR' AND character_maximum_length = 255;");

            List<String> stringColumns = fetchColumnValues(resultSet, "column_name");

            assertThat(stringColumns.size(), is(3));
            assertThat(stringColumns, containsInAnyOrder("email", "first_name", "last_name"));
        }
    }

    @Test
    public void testUserBirthdayTypeIsDate() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'users' AND column_name = 'birthday';");

            resultSet.next();
            String idTypeName = resultSet.getString("type_name");

            assertThat(idTypeName, is("DATE"));
        }
    }

    // table sale_group test

    @Test
    public void testProfilesTablesHasPrimaryKey() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'profiles' AND constraint_type = 'PRIMARY_KEY';");

            boolean resultIsNotEmpty = resultSet.next();

            assertThat(resultIsNotEmpty, is(true));
        }
    }

    @Test
    public void testProfilesTablePrimaryKeyHasCorrectName() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'profiles' AND constraint_type = 'PRIMARY_KEY';");

            resultSet.next();
            String pkConstraintName = resultSet.getString("constraint_name");

            assertThat(pkConstraintName, equalTo("profiles_PK"));
        }
    }

    @Test
    public void testProfilesTablePrimaryKeyBasedOnForeignKeyColumn() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'profiles' AND constraint_type = 'PRIMARY_KEY';");

            resultSet.next();
            String pkColumn = resultSet.getString("column_list");

            assertThat("user_id", equalTo(pkColumn));
        }
    }

    @Test
    public void testProfilesTableHasAllRequiredColumns() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'profiles';");

            List<String> columns = fetchColumnValues(resultSet, "column_name");

            assertThat(columns.size(), equalTo(5));
            assertThat(columns, containsInAnyOrder("user_id", "job_position", "company", "education", "city"));
        }
    }

    @Test
    public void testProfilesGroupIdTypeIsBigint() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'profiles' AND column_name = 'user_id';");

            resultSet.next();
            String idTypeName = resultSet.getString("type_name");

            assertThat(idTypeName, is("BIGINT"));
        }
    }

    @Test
    public void testProfilesTableStringColumnsHaveCorrectTypeAndLength() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS" +
                    " WHERE table_name = 'profiles' AND type_name = 'VARCHAR' AND character_maximum_length = 255;");

            List<String> stringColumns = fetchColumnValues(resultSet, "column_name");

            assertThat(stringColumns.size(), is(4));
            assertThat(stringColumns, containsInAnyOrder("job_position", "company", "education", "city"));
        }
    }

    @Test
    public void testProfilesHasForeignKeyToUsers() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'profiles' AND constraint_type = 'REFERENTIAL' AND column_list = 'user_id';");

            boolean resultIsNotEmpty = resultSet.next();

            assertThat(resultIsNotEmpty, is(true));
        }
    }

    @Test
    public void testProfilesForeignKeyToUsersHasCorrectName() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.CONSTRAINTS" +
                    " WHERE table_name = 'profiles' AND constraint_type = 'REFERENTIAL' AND column_list = 'user_id';");

            resultSet.next();
            String fkConstraintName = resultSet.getString("constraint_name");

            assertThat(fkConstraintName, equalTo("profiles_users_FK"));
        }
    }
}
