package com.bobocode.dao;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;

public class ProductDaoImpl implements ProductDao {
    private static final String SAVE_SQL = "INSERT INTO products(name, producer, price, expiration_date)" +
            " VALUES(?, ?, ?, ?)";
    private static final String FIND_ALL_SQL = "SELECT * FROM products";
    private static final String FIND_BY_ID__SQL = "SELECT * FROM products WHERE id = ?";
    private static final String UPDATE_SQL = "UPDATE products SET name=?, producer=?, price=?, expiration_date=?" +
            " WHERE id=?";
    private static final String REMOVE_BY_ID_SQL = "DELETE FROM products WHERE id=?";
    private DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(SAVE_SQL, Statement.RETURN_GENERATED_KEYS);
            setStatementParameters(statement, product);
            statement.executeUpdate();
            insertGeneratedKeyToProduct(product, statement);
        } catch (SQLException e) {
            String messagee = String.format("Error saving product: %s", product);
            throw new DaoOperationException(messagee, e.getCause());
        }
    }

    @Override
    public List<Product> findAll() {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet allRows = statement.executeQuery(FIND_ALL_SQL);
            return resultSetToListOfProducts(allRows);
        } catch (SQLException e) {
            throw new DaoOperationException("Find all failed", e.getCause());
        }
    }

    @Override
    public Product findOne(Long id) {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(FIND_BY_ID__SQL);
            statement.setLong(1, id);
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            return productFromResultSet(resultSet);
        } catch (SQLException e) {
            String message = String.format("Product with id = %d does not exist", id);
            throw new DaoOperationException(message, e.getCause());
        }
    }

    @Override
    public void update(Product product) {
        idRequiresNonNull(product);
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(UPDATE_SQL);
            setStatementParameters(statement, product);
            int rowsAffected = statement.executeUpdate();
            if (rowsAffected == 0) {
                throw new SQLException();
            }
        } catch (SQLException e) {
            String message = String.format("Product with id = %d does not exist", product.getId());
            throw new DaoOperationException(message, e.getCause());
        }
    }

    @Override
    public void remove(Product product) {
        idRequiresNonNull(product);
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(REMOVE_BY_ID_SQL);
            statement.setLong(1, product.getId());
            int rowsAffected = statement.executeUpdate();
            if (rowsAffected == 0) {
                throw new SQLException();
            }
        } catch (SQLException e) {
            String message = String.format("Product with id = %d does not exist", product.getId());
            throw new DaoOperationException(message, e.getCause());
        }
    }

    private void setStatementParameters(PreparedStatement statement, Product product) throws SQLException {
        statement.setString(1, product.getName());
        statement.setString(2, product.getProducer());
        statement.setBigDecimal(3, product.getPrice());
        Date expirationDate = Date.valueOf(product.getExpirationDate());
        statement.setDate(4, expirationDate);
        int numberOfStatementParam = statement.getParameterMetaData().getParameterCount();
        if (numberOfStatementParam == 5) {
            statement.setLong(5, product.getId());
        }
    }

    private Product productFromResultSet(ResultSet resultSet) throws SQLException {
        return Product.builder()
                .id(resultSet.getLong("id"))
                .name(resultSet.getString("name"))
                .producer(resultSet.getString("producer"))
                .price(resultSet.getBigDecimal("price"))
                .expirationDate(resultSet.getDate("expiration_date").toLocalDate())
                .creationTime(resultSet.getTimestamp("creation_time").toLocalDateTime())
                .build();
    }

    private void insertGeneratedKeyToProduct(Product product, PreparedStatement statement) throws SQLException {
        ResultSet generatedKey = statement.getGeneratedKeys();
        if (generatedKey.next()) {
            long id = generatedKey.getLong(1);
            product.setId(id);
        }
    }

    private List<Product> resultSetToListOfProducts(ResultSet allRows) throws SQLException {
        List<Product> products = new ArrayList<>();
        while (allRows.next()) {
            Product product = productFromResultSet(allRows);
            products.add(product);
        }
        return products;
    }

    private void idRequiresNonNull(Product product) {
        if (product.getId() == null) {
            throw new DaoOperationException("Cannot find a product without ID");
        }
    }

}
