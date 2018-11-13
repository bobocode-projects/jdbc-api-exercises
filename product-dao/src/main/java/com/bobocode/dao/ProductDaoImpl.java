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
    private static final String SAVE_PRODUCT_QUERY = "INSERT INTO products(name, producer, price, expiration_date) VALUES (?,?,?,?);";
    private static final String LIST_ALL_PRODUCTS_QUERY = "SELECT * FROM products;";
    private static final String FIND_PRODUCT_BY_ID_QUERY = "SELECT * FROM products where id = ?;";
    private static final String UPDATE_PRODUCT_BY_ID_QUERY = "UPDATE products SET name = ?, producer = ?, price = ?, expiration_date = ? WHERE id = ?;";
    private static final String REMOVE_PRODUCT_BY_ID_QUERY = "DELETE FROM products where id = ?;";

    private DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement saveProduct = connection.prepareStatement(SAVE_PRODUCT_QUERY, PreparedStatement.RETURN_GENERATED_KEYS);
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
            PreparedStatement findALL = connection.prepareStatement(LIST_ALL_PRODUCTS_QUERY);
            ResultSet findAllResultSet = findALL.executeQuery();
            products = getProducts(findAllResultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return products;
    }

    private static List<Product> getProducts(ResultSet findedProducts) throws SQLException {
        List<Product> listOfProducts = new ArrayList<>();
        while (findedProducts.next()) {
            Product product = getOneProduct(findedProducts);
            listOfProducts.add(product);
        }
        return listOfProducts;
    }

    @Override
    public Product findOne(Long id) {
        Product product = null;
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(FIND_PRODUCT_BY_ID_QUERY);
            preparedStatement.setLong(1, id);
            ResultSet findProduct = preparedStatement.executeQuery();
            if (findProduct.next()) {
                product = getOneProduct(findProduct);
            } else {
                throw new DaoOperationException("Product with id = " + id + " does not exist");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return product;
    }

    private static Product getOneProduct(ResultSet data) throws SQLException {
        Product product = new Product();
        product.setId(data.getLong(1));
        product.setName(data.getString(2));
        product.setProducer(data.getString(3));
        product.setPrice(data.getBigDecimal(4));
        product.setExpirationDate(data.getDate(5).toLocalDate());
        product.setCreationTime(data.getDate(6).toLocalDate().atStartOfDay());
        return product;
    }

    @Override
    public void update(Product product) {
        checkIdIsPresent(product.getId());
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement updateStatement = connection.prepareStatement(UPDATE_PRODUCT_BY_ID_QUERY);
            setValues(updateStatement, product);
            updateStatement.setLong(5, product.getId());
            executeUpdate(updateStatement, product.getId());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void remove(Product product) {
        checkIdIsPresent(product.getId());
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement removeProduct = connection.prepareStatement(REMOVE_PRODUCT_BY_ID_QUERY);
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

    private static void checkIdIsPresent(Object id) {
        if (id == null) {
            throw new DaoOperationException("Cannot find a product without ID");
        }
    }
}
