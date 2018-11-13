package com.bobocode.dao;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class ProductDaoImpl implements ProductDao {
    private static final String INSERT_SQL = "INSERT INTO products(name, producer, price, expiration_date) VALUES (?, ?, ?, ?);";
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
            throw new DaoOperationException("product wasn't saved!!!");
        }
    }

    @Override
    public List<Product> findAll() {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT * FROM products;");
            List<Product> allProducts = new ArrayList<>();
            while (resultSet.next()) {
                Product product = new Product();
                product.setId(resultSet.getLong("id"));
                product.setName(resultSet.getString("name"));
                product.setProducer(resultSet.getString("producer"));
                product.setPrice(resultSet.getBigDecimal("price"));
                product.setExpirationDate(resultSet.getDate("expiration_date").toLocalDate());
                product.setCreationTime(resultSet.getTimestamp("creation_time").toLocalDateTime());
                allProducts.add(product);
            }
            return allProducts;
        } catch (SQLException e) {
            throw new DaoOperationException("can't find product by id");
        }
    }

    @Override
    public Product findOne(Long id) {
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
