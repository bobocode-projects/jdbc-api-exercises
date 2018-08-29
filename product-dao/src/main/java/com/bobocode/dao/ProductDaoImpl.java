package com.bobocode.dao;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ProductDaoImpl implements ProductDao {

    public static final String INSERT_SQL = "INSERT INTO products (name, producer, price, expiration_date) VALUES (?, ?, ?, ?);";
    public static final String SELECT_ALL_SQL = "SELECT * FROM products;";
    public static final String SELECT_BY_ID_SQL = "SELECT * FROM products WHERE id = ?";
    public static final String UPDATE_BY_ID_SQL = "UPDATE products SET name = ?, producer = ?, price = ?, expiration_date = ? WHERE id = ?;";
    public static final String DELETE_BY_ID_SQL = "DELETE FROM products WHERE id = ?;";

    private DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {
        Objects.requireNonNull(product);
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement preparedStatement = factoryPrepareStatement(INSERT_SQL, connection, product);
            executeUpdate(preparedStatement);
            Long productId = fetchGeneratedId(preparedStatement);
            product.setId(productId);
        } catch (SQLException e) {
            throw new DaoOperationException(String.format("Error saving product: %s", product), e);
        }
    }

    private void executeUpdate(PreparedStatement preparedStatement) throws SQLException {
        int rowsAffected = preparedStatement.executeUpdate();
        if (rowsAffected == 0) {
            throw new DaoOperationException("Nothing has been changed");
        }
    }

    private Long fetchGeneratedId(PreparedStatement insertStatement) throws SQLException {
        ResultSet generatedKeys = insertStatement.getGeneratedKeys();
        if (generatedKeys.next()) {
            return generatedKeys.getLong("id");
        } else {
            throw new DaoOperationException("Can not obtain product ID");
        }
    }

    @Override
    public List<Product> findAll() {
        List<Product> products = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(SELECT_ALL_SQL);
            while (resultSet.next()) {
                Product product = mapProductFromResultSet(resultSet);
                products.add(product);
            }
        } catch (SQLException e) {
            throw new DaoOperationException("Error finding all products", e);
        }
        return products;
    }

    private Product mapProductFromResultSet(ResultSet resultSet) throws SQLException {
        Product product = new Product();
        product.setId(resultSet.getLong("id"));
        product.setName(resultSet.getString("name"));
        product.setProducer(resultSet.getString("producer"));
        product.setPrice(resultSet.getBigDecimal("price"));
        product.setExpirationDate(resultSet.getDate("expiration_date").toLocalDate());
        product.setCreationTime(resultSet.getTimestamp("creation_time").toLocalDateTime());
        return product;
    }

    @Override
    public Product findOne(Long id) {
        Objects.requireNonNull(id);
        try (Connection connection = dataSource.getConnection()) {
            return findById(id, connection);
        } catch (SQLException e) {
            throw new DaoOperationException(String.format("Error finding product by id = %d", id), e);
        }
    }

    private Product findById(Long id, Connection connection) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(SELECT_BY_ID_SQL);
        preparedStatement.setLong(1, id);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            return mapProductFromResultSet(resultSet);
        } else {
            throw new DaoOperationException(String.format("Product with id = %d does not exist", id));
        }
    }

    @Override
    public void update(Product product) {
        Objects.requireNonNull(product);
        try (Connection connection = dataSource.getConnection()) {
            checkIsExistsInDB(product, connection);
            PreparedStatement preparedStatement = factoryPrepareStatement(UPDATE_BY_ID_SQL, connection, product);
            executeUpdate(preparedStatement);
        } catch (SQLException e) {
            throw new DaoOperationException(String.format("Error updating product: %s", product), e);
        }
    }

    private void checkIsExistsInDB(Product product, Connection connection) throws SQLException {
        if (product.getId() == null) {
            throw new DaoOperationException("Cannot find a product without ID");
        }
        findById(product.getId(), connection);
    }

    @Override
    public void remove(Product product) {
        Objects.requireNonNull(product);
        try (Connection connection = dataSource.getConnection()) {
            checkIsExistsInDB(product, connection);
            PreparedStatement preparedStatement = factoryPrepareStatement(DELETE_BY_ID_SQL, connection, product);
            executeUpdate(preparedStatement);
        } catch (SQLException e) {
            throw new DaoOperationException(String.format("Error removing product by id = %d", product.getId()), e);
        }
    }

    private PreparedStatement factoryPrepareStatement(String sql, Connection connection, Product product) throws SQLException {
        Objects.requireNonNull(sql);
        switch (sql) {
            case INSERT_SQL: {
                PreparedStatement preparedStatement = connection.prepareStatement(INSERT_SQL, PreparedStatement.RETURN_GENERATED_KEYS);
                preparedStatement.setString(1, product.getName());
                preparedStatement.setString(2, product.getProducer());
                preparedStatement.setBigDecimal(3, product.getPrice());
                preparedStatement.setDate(4, Date.valueOf(product.getExpirationDate()));
                return preparedStatement;
            }
            case UPDATE_BY_ID_SQL: {
                PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_BY_ID_SQL);
                preparedStatement.setString(1, product.getName());
                preparedStatement.setString(2, product.getProducer());
                preparedStatement.setBigDecimal(3, product.getPrice());
                preparedStatement.setDate(4, Date.valueOf(product.getExpirationDate()));
                preparedStatement.setLong(5, product.getId());
                return preparedStatement;
            }
            case DELETE_BY_ID_SQL: {
                PreparedStatement preparedStatement = connection.prepareStatement(DELETE_BY_ID_SQL);
                preparedStatement.setLong(1, product.getId());
                return preparedStatement;
            }
            default:
                throw new DaoOperationException(String.format("Cannot prepare statement for product: %s", product));
        }
    }
}
