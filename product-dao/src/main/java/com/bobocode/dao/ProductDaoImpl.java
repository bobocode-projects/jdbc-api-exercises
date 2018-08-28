package com.bobocode.dao;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class ProductDaoImpl implements ProductDao {
    private DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {

        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "INSERT INTO products (name, producer, price, expiration_date) VALUES (?, ?, ?, ?);",
                    PreparedStatement.RETURN_GENERATED_KEYS);
            preparedStatement.setString(1, product.getName());
            preparedStatement.setString(2, product.getProducer());
            preparedStatement.setBigDecimal(3, product.getPrice());
            preparedStatement.setDate(4, Date.valueOf(product.getExpirationDate()));
            int i = preparedStatement.executeUpdate();
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            Long productId = null;
            if (generatedKeys.next()) {
                productId = generatedKeys.getLong("id");
            }
            if (productId != null) {
                product.setId(productId);
            }
        } catch (SQLException e) {
            throw new DaoOperationException(String.format("Error saving product: %s", product), e);
        }
    }

    @Override
    public List<Product> findAll() {

        List<Product> products = new ArrayList<>();

        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM products;");
            while (resultSet.next()) {
                Product product = new Product();
                product.setId(resultSet.getLong("id"));
                product.setName(resultSet.getString("name"));
                product.setProducer(resultSet.getString("producer"));
                product.setPrice(resultSet.getBigDecimal("price"));
                product.setExpirationDate(resultSet.getDate("expiration_date").toLocalDate());
                product.setCreationTime(resultSet.getTimestamp("creation_time").toLocalDateTime());

                products.add(product);
            }
        } catch (SQLException e) {
            throw new DaoOperationException(e.getMessage(), e);
        }

        return products;
    }

    @Override
    public Product findOne(Long id) {

        Product product = null;

        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM products WHERE id = ?");
            preparedStatement.setLong(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                product = new Product();
                product.setId(resultSet.getLong("id"));
                product.setName(resultSet.getString("name"));
                product.setProducer(resultSet.getString("producer"));
                product.setPrice(resultSet.getBigDecimal("price"));
                product.setExpirationDate(resultSet.getDate("expiration_date").toLocalDate());
                product.setCreationTime(resultSet.getTimestamp("creation_time").toLocalDateTime());
            } else {
                throw new DaoOperationException(String.format("Product with id = %d does not exist", id));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return product;
    }

    @Override
    public void update(Product product) {

        if (product.getId() == null) {
            throw new DaoOperationException("Cannot find a product without ID");
        }

        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "UPDATE products SET name = ?, producer = ?, price = ?, expiration_date = ? WHERE id = ?;");
            preparedStatement.setString(1, product.getName());
            preparedStatement.setString(2, product.getProducer());
            preparedStatement.setBigDecimal(3, product.getPrice());
            preparedStatement.setDate(4, Date.valueOf(product.getExpirationDate()));
            preparedStatement.setLong(5, product.getId());
            int i = preparedStatement.executeUpdate();
            if (i == 0){
                throw new DaoOperationException(String.format("Product with id = %d does not exist", product.getId()));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void remove(Product product) {

        if (product.getId() == null) {
            throw new DaoOperationException("Cannot find a product without ID");
        }

        try (Connection connection = dataSource.getConnection()) {

            PreparedStatement preparedStatement = connection.prepareStatement(
                    "DELETE FROM products WHERE id = ?;");
            preparedStatement.setLong(1, product.getId());
            int i = preparedStatement.executeUpdate();
            if (i == 0) {
                throw new DaoOperationException(String.format("Product with id = %d does not exist", product.getId()));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
