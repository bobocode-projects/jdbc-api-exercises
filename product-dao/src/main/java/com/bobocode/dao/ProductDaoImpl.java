package com.bobocode.dao;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.isNull;

public class ProductDaoImpl implements ProductDao {
    private DataSource dataSource;

    private static final String FIND_ALL_QUERY = "select * from products";
    private static final String FIND_ONE_BY_ID_QUERY = "select * from products where id = ?";
    private static final String UPDATE_QUERY = "update products set name = ?, producer = ?," +
            "price = ?, expiration_date = ? where id = ?";
    private static final String REMOVE_PRODUCT_QUERY = "delete from products where id = ?";
    private static final String INSERT_PRODUCT_QUERY = "insert into " +
            "products (name, producer, price, expiration_date) " +
            "values (?, ?, ?, ?)";

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {
        try(Connection connection = dataSource.getConnection()) {
            save(product, connection);
        } catch (SQLException e) {
            throw new DaoOperationException(format("Error saving product: %s", product));
        }
    }

    private void save(Product product, Connection connection) throws SQLException {
        PreparedStatement statement = prepareSaveStatement(connection, product);
        fetchInsertStatementId(product, statement);
    }

    private PreparedStatement prepareSaveStatement(Connection connection, Product product) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(INSERT_PRODUCT_QUERY, PreparedStatement.RETURN_GENERATED_KEYS);
        statement.setString(1, product.getName());
        statement.setString(2, product.getProducer());
        statement.setBigDecimal(3, product.getPrice());
        statement.setDate(4, Date.valueOf(product.getExpirationDate()));
        statement.executeUpdate();
        return statement;
    }

    private void fetchInsertStatementId(Product product, PreparedStatement statement) throws SQLException {
        ResultSet resultSet = statement.getGeneratedKeys();
        if (resultSet.next()) {
            product.setId(resultSet.getLong(1));
        } else {
            throw new DaoOperationException("Failed to save product");
        }
    }

    @Override
    public List<Product> findAll() {
        try(Connection connection = dataSource.getConnection()) {
            return findAll(connection);
        } catch (SQLException e) {
            throw new DaoOperationException(format("Failed to get all element, ex: %s", e.getMessage()), e);
        }
    }

    private List<Product> findAll(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(FIND_ALL_QUERY);
        return mapToProductList(resultSet);
    }

    private List<Product> mapToProductList(ResultSet resultSet) throws SQLException {
        List<Product> list = new ArrayList<>();

        while (resultSet.next()){
            Product product = mapToProduct(resultSet);
            list.add(product);
        }

        return list;
    }

    @Override
    public Product findOne(Long id) {
        try(Connection connection = dataSource.getConnection()) {
            return findOne(connection, id);
        } catch (SQLException e) {
            throw new DaoOperationException(format("Failed to get all element, ex: %s", e.getMessage()), e);
        }
    }

    private Product findOne(Connection connection, Long id) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(FIND_ONE_BY_ID_QUERY);
        statement.setLong(1, id);
        ResultSet resultSet = statement.executeQuery();
        return validateFindOneResult(resultSet, id);
    }

    private Product validateFindOneResult(ResultSet resultSet, Long id) throws SQLException {
        boolean isProductPresent = resultSet.next();
        if (isProductPresent) {
            return mapToProduct(resultSet);
        } else {
            throw new DaoOperationException(format("Product with id = %d does not exist", id));
        }
    }

    private Product mapToProduct(ResultSet resultSet) throws SQLException {
        return Product.builder()
                .id(resultSet.getLong(1))
                .name(resultSet.getString(2))
                .producer(resultSet.getString(3))
                .price(resultSet.getBigDecimal(4))
                .expirationDate(resultSet.getDate(5).toLocalDate())
                .creationTime(resultSet.getDate(6).toLocalDate().atStartOfDay())
                .build();
    }

    @Override
    public void update(Product product) {
        try(Connection connection = dataSource.getConnection()){
            validateProduct(product);
            update(connection, product);
        } catch (SQLException e) {
            throw new DaoOperationException(format("Failed to get all element, ex: %s", e.getMessage()), e);
        }
    }

    private void update(Connection connection, Product product) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(UPDATE_QUERY);
        statement.setString(1, product.getName());
        statement.setString(2, product.getProducer());
        statement.setBigDecimal(3, product.getPrice());
        statement.setDate(4, Date.valueOf(product.getExpirationDate()));
        statement.setLong(5, product.getId());
        statement.executeUpdate();
    }

    @Override
    public void remove(Product product) {
        try(Connection connection = dataSource.getConnection()){
            validateProduct(product);
            remove(connection, product);
        } catch (SQLException e) {
            throw new DaoOperationException(format("Product with id = %d does not exist", product.getId()));
        }
    }

    private void remove(Connection connection, Product product) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(REMOVE_PRODUCT_QUERY);
        statement.setLong(1, product.getId());
        statement.executeUpdate();
    }

    private void validateProduct(Product product) {
        if (isNull(product.getId())) {
            throw new DaoOperationException("Cannot find a product without ID");
        }
    }

}
