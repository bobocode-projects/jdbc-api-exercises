package com.bobocode.dao;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;
import org.h2.command.Prepared;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class ProductDaoImpl implements ProductDao {
    private static final String SAVE_PRODUCT = "INSERT INTO products(name, producer, price, expiration_date) VALUES (?,?,?,?);";
    private static final String LIST_ALL_PRODUCTS = "SELECT * FROM products;";
    private static final String FIND_PRODUCT_BY_ID = "SELECT * FROM products where id = ?;";
    private static final String UPDATE_PRODUCT_BY_ID = "UPDATE products SET name = ?, producer = ?, price = ?, expiration_date = ? WHERE id = ?;";
    private static final String REMOVE_PRODUCT_BY_ID = "DELETE FROM products where id = ?;";

    private DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement saveProduct = connection.prepareStatement(SAVE_PRODUCT, PreparedStatement.RETURN_GENERATED_KEYS);
            setValues(saveProduct, product);
            saveProduct.executeUpdate();
            Long generatedId = getGeneratedKey(saveProduct);
            product.setId(generatedId);

        } catch (SQLException e) {
            throw new DaoOperationException(String.format("Error saving product: %s", product));
        }
    }

    private static void setValues(PreparedStatement saveStatement, Product product) throws SQLException {
        saveStatement.setString(1, product.getName());
        saveStatement.setString(2, product.getProducer());
        saveStatement.setInt(3, product.getPrice().intValue());
        saveStatement.setDate(4, Date.valueOf(product.getExpirationDate()));
    }

    private static Long getGeneratedKey(PreparedStatement saveStatement) throws SQLException {
        ResultSet generatedKey = saveStatement.getGeneratedKeys();
        generatedKey.next();
        return generatedKey.getLong(1);
    }

    @Override
    public List<Product> findAll() {
        List<Product> products = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement findALL = connection.prepareStatement(LIST_ALL_PRODUCTS);
            ResultSet findAllResultSet = findALL.executeQuery();
            while (findAllResultSet.next()) {
                Product product = new Product();
                product.setId(findAllResultSet.getLong(1));
                product.setName(findAllResultSet.getString(2));
                product.setProducer(findAllResultSet.getString(3));
                product.setPrice(findAllResultSet.getBigDecimal(4));
                product.setCreationTime(findAllResultSet.getDate(5).toLocalDate().atStartOfDay());
                product.setExpirationDate(findAllResultSet.getDate(6).toLocalDate());
                products.add(product);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return products;
    }

    @Override
    public Product findOne(Long id) {
        Product product = null;
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(FIND_PRODUCT_BY_ID);
            preparedStatement.setLong(1, id);
            ResultSet findProduct = preparedStatement.executeQuery();
            product = getProduct(findProduct, id);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return product;
    }

    private static Product getProduct(ResultSet data, Long id) throws SQLException {
        Product product = new Product();
        if (data.next()) {
            product.setId(data.getLong(1));
            product.setName(data.getString(2));
            product.setProducer(data.getString(3));
            product.setPrice(data.getBigDecimal(4));
            product.setExpirationDate(data.getDate(5).toLocalDate());
            product.setCreationTime(data.getDate(6).toLocalDate().atStartOfDay());
        } else {
            throw new DaoOperationException("Product with id = " + id + " does not exist");
        }
        return product;
    }

    @Override
    public void update(Product product) {
        checkIndexIsPresent(product.getId());
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement updateProduct = connection.prepareStatement(UPDATE_PRODUCT_BY_ID);
            updateProduct.setString(1, product.getName());
            updateProduct.setString(2, product.getProducer());
            updateProduct.setBigDecimal(3, product.getPrice());
            updateProduct.setDate(4, Date.valueOf(product.getExpirationDate()));
            updateProduct.setLong(5, product.getId());
            executeUpdate(updateProduct, product.getId());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void remove(Product product) {
        checkIndexIsPresent(product.getId());
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement removeProduct = connection.prepareStatement(REMOVE_PRODUCT_BY_ID);
            removeProduct.setLong(1, product.getId());
            executeUpdate(removeProduct, product.getId());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void executeUpdate(PreparedStatement statement, Long id) throws SQLException {
        int rowsAffected = statement.executeUpdate();
        if (rowsAffected == 0) {
            throw new DaoOperationException(String.format("Product with id = %d does not exist", id));
        }
    }

    private static void checkIndexIsPresent(Object id) {
        if (id == null) {
            throw new DaoOperationException("Cannot find a product without ID");
        }
    }
}
