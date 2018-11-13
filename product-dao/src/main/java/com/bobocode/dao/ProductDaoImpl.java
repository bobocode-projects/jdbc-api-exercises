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
            statement.setString(1, product.getName());
            statement.setString(2, product.getProducer());
            statement.setBigDecimal(3, product.getPrice());
            Date expirationDate = Date.valueOf(product.getExpirationDate());
            statement.setDate(4, expirationDate);
            statement.executeUpdate();
            ResultSet generatedKey = statement.getGeneratedKeys();
            if (generatedKey.next()) {
                long id = generatedKey.getLong(1);
                product.setId(id);
            }
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
            List<Product> products = new ArrayList<>();
            while (allRows.next()) {
                Product product = new Product();
                product.setId(allRows.getLong("id"));
                product.setName(allRows.getString("name"));
                product.setProducer(allRows.getString("producer"));
                product.setPrice(allRows.getBigDecimal("price"));
                product.setExpirationDate(allRows.getDate("expiration_date").toLocalDate());
                product.setCreationTime(allRows.getTimestamp("creation_time").toLocalDateTime());
                products.add(product);
            }
            return products;
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
            Product product = new Product();
            product.setId(resultSet.getLong("id"));
            product.setName(resultSet.getString("name"));
            product.setProducer(resultSet.getString("producer"));
            product.setPrice(resultSet.getBigDecimal("price"));
            product.setExpirationDate(resultSet.getDate("expiration_date").toLocalDate());
            product.setCreationTime(resultSet.getTimestamp("creation_time").toLocalDateTime());
            return product;
        } catch (SQLException e) {
            String message = String.format("Product with id = %d does not exist", id);
            throw new DaoOperationException(message, e.getCause());
        }
    }

    @Override
    public void update(Product product) {
        if (product.getId() == null) {
            throw new DaoOperationException("Cannot find a product without ID");
        }
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(UPDATE_SQL);
            statement.setString(1, product.getName());
            statement.setString(2, product.getProducer());
            statement.setBigDecimal(3, product.getPrice());
            Date expirationDate = Date.valueOf(product.getExpirationDate());
            statement.setDate(4, expirationDate);
            statement.setLong(5, product.getId());
            int affectedRows = statement.executeUpdate();
            if (affectedRows == 0) {
                throw new SQLException();
            }
        } catch (SQLException e) {
            String message = String.format("Product with id = %d does not exist", product.getId());
            throw new DaoOperationException(message, e.getCause());
        }
    }

    @Override
    public void remove(Product product) {
        if (product.getId() == null) {
            throw new DaoOperationException("Cannot find a product without ID");
        }
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement statement = connection.prepareStatement(REMOVE_BY_ID_SQL);
            statement.setLong(1, product.getId());
            int affectedRows = statement.executeUpdate();
            if (affectedRows == 0) {
                throw new SQLException();
            }
        } catch (SQLException e) {
            String message = String.format("Product with id = %d does not exist", product.getId());
            throw new DaoOperationException(message, e.getCause());
        }
    }

}
