

drop table if exists customer;
drop table if exists store;
drop table if exists supplier;
drop table if exists product;
drop table if exists datedim;
drop table if exists fact_Sales;



create table customer(
	customerid int primary key,
    gender char(1),
    age_range varchar(10),
    occupation int,
    city_category varchar(1),
    stay_years int,
    martial_status int
);

create table store(
	storeid int primary key,
    storeName varchar(100)
);

create table supplier(
	supplierid int primary key,
    supplierName varchar(100)
); 

create table product(
	productid varchar(100) primary key,
    productCategory varchar(280),
    price decimal(10,2),
    supplierid int,
    storeid int,
    foreign key (supplierid) references supplier(supplierid),
    foreign key (storeid) references store(storeid)
);


create table dateDim(
	dateid int primary key auto_increment,
    date Date,
    day int,
    month int,
    year int,
    quarter int,
    weekday varchar(15),
    weekend_flag boolean,
    season varchar(15),
    half_year varchar(2)
);

DROP TABLE IF EXISTS fact_sales;

CREATE TABLE fact_sales (
    saleid INT PRIMARY KEY AUTO_INCREMENT,
    orderid INT NOT NULL UNIQUE,
    date DATE,
    quantity INT NOT NULL,

    customerid INT,
    productid VARCHAR(100),
    storeid INT,
    supplierid INT,
    dateid INT,

    Gender CHAR(1),
    Age VARCHAR(20),
    Occupation INT,
    City_Category VARCHAR(2),
    Stay_In_Current_City_Years INT,
    Marital_Status INT,

    Product_Category VARCHAR(280),
    price DECIMAL(10,2),
    StoreName VARCHAR(100),
    SupplierName VARCHAR(100),

    total_amount DECIMAL(12,2),
    
    FOREIGN KEY (customerid) REFERENCES customer(customerid),
    FOREIGN KEY (productid) REFERENCES product(productid),
    FOREIGN KEY (dateid) REFERENCES dateDim(dateid)
);


