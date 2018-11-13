package com.bobocode.dao;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ProductDaoImpl implements ProductDao {
    private static final String INSERT_SQL = "INSERT INTO products " +
            "(name, producer, price, expiration_date) VALUES (?,?,?,?);";
    private static final String GET_ALL_PRODUCTS_SQL = "SELECT * FROM products;";
    private static final String FIND_ONE_BY_ID_SQL = "SELECT * FROM products  WHERE id = ?";
    private DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(INSERT_SQL, Statement.RETURN_GENERATED_KEYS);
            preparedStatement.setString(1, product.getName());
            preparedStatement.setString(2, product.getProducer());
            preparedStatement.setBigDecimal(3, product.getPrice());
            preparedStatement.setDate(4, Date.valueOf(product.getExpirationDate()));
            preparedStatement.executeUpdate();
            ResultSet resultSet = preparedStatement.getGeneratedKeys();
            resultSet.next();
            long id = resultSet.getLong(1);
            product.setId(id);
        } catch (SQLException e) {
            throw new DaoOperationException("product was not saved!");
        }
    }

    @Override
    public List<Product> findAll() {
        List<Product> products = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(GET_ALL_PRODUCTS_SQL);
            while (resultSet.next()) {
                Product myProduct = new Product();
                myProduct.setId(resultSet.getLong("id"));
                myProduct.setName(resultSet.getString("name"));
                myProduct.setProducer(resultSet.getString("producer"));
                myProduct.setPrice(resultSet.getBigDecimal("price"));
                myProduct.setExpirationDate(resultSet.getDate("expiration_date").toLocalDate());
                myProduct.setCreationTime(resultSet.getTimestamp("creation_time").toLocalDateTime());
                products.add(myProduct);
            }
            return products;
        } catch (SQLException e) {
            throw new DaoOperationException("cant find any products");
        }
    }

    @Override
    public Product findOne(Long id) {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(FIND_ONE_BY_ID_SQL);
            preparedStatement.setLong(1, id);
            Product product = new Product();
            ResultSet resultSet = preparedStatement.executeQuery();
        } catch (SQLException e) {
            throw new DaoOperationException("cant find one by id");
        }
        return null;
    }

    @Override
    public void update(Product product) {
        throw new UnsupportedOperationException("None of these methods will work unless you implement them!");// todo
    }

    @Override
    public void remove(Product product) {
        throw new UnsupportedOperationException("None of these methods will work unless you implement them!");// todo
    }

}
