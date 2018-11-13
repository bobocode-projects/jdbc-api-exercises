CREATE TABLE broker(
id BIGINT,
username varchar(255) NOT NULL,
first_name varchar(255) NOT NULL,
last_name varchar(255) NOT NULL,
CONSTRAINT PK_broker PRIMARY KEY (id),
CONSTRAINT UQ_broker_username UNIQUE (username)
);

CREATE TABLE sales_group(
id BIGINT,
name varchar(255) NOT NULL,
transaction_type varchar(255) NOT NULL,
max_transaction_amount DECIMAL NOT NULL,
CONSTRAINT PK_sales_group PRIMARY KEY (id),
CONSTRAINT UQ_sales_group_name UNIQUE (name)
);

CREATE TABLE broker_sales_group(
broker_id BIGINT,
sales_group_id BIGINT,
CONSTRAINT FK_broker_sales_group_broker FOREIGN KEY (broker_id) references broker,
CONSTRAINT FK_broker_sales_group_sales_group FOREIGN KEY (sales_group_id) references sales_group,
CONSTRAINT PK_broker_sales_group PRIMARY KEY (broker_id, sales_group_id)
);

/*

WallStreet database should store information about brokers, sales groups and its relations.

Each broker must have a unique username. First and last names are also mandatory.

A sales group is a special group that has its own restrictions. Sale groups are used to organize the work of brokers.
Each group mush have a unique name, transaction type (string), and max transaction amount (a number). All field are
mandatory.

A sales group can consists of more than one broker, while each broker can be associated with more than one sale group.

  TECH NOTES AND NAMING CONVENTION
- All tables, columns and constraints are named using "snake case" naming convention
- All table names must be singular (e.g. "user", not "users")
- All tables (except link tables) should have an id of type BIGINT, which is a primary key
- Link tables should have composite primary key, that consists of two other foreign key columns
- All primary key, foreign key, and unique constraint should be named according to the naming convention.
- All link tables should have a composite key that consists of two foreign key columns

- All primary keys should be named according to the following rule "PK_table_name"
- All foreign keys should be named according to the following rule "FK_table_name_reference_table_name"
- All alternative keys (unique) should be named according to the following rule "UQ_table_name_column_name"

*/

-- TODO: write SQL script to create a database tables according to the requirements
