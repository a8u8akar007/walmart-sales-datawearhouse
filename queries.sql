use db11;
----------- QUERIES -----------
-- Q1

select product_category, month, day_type, revenue, rnk
from (
    select f.product_category, month(d.date) as month, case when d.weekend_flag = 1 then 'Weekend' else 'Weekday' end as day_type, sum(f.total_amount) as revenue,
	row_number() over ( partition by month(d.date), case when d.weekend_flag = 1 then 'Weekend' else 'Weekday' end
            order by sum(f.total_amount) desc ) as rnk
    from fact_sales f
    join dateDim d on f.dateid = d.dateid
    group by f.product_category, month(d.date), day_type
) t
where rnk <= 5
order by month, day_type, revenue desc;


-- Q2

select Gender, Age, City_Category, SUM(total_amount) as total_purchase
from fact_sales
group by Gender, Age, City_Category
order by Gender, Age, City_Category, total_purchase DESC;

-- Q3
select  occupation, Product_Category, SUM(total_amount) as total_sales
from fact_sales
group by Occupation, Product_Category
order by Product_Category, Occupation, total_sales desc;

-- Q4
select Gender, Age, d.quarter, SUM(total_amount) AS QuartSales
from fact_sales f
join dateDim d ON f.dateid = d.dateid
where d.year = 2020
group by Gender, Age, d.quarter
order by Gender, Age, d.quarter;


-- Q5

select product_category, occupation, total_sales
from (
    select product_category, occupation, sum(total_amount) as total_sales,
	row_number() over (partition by product_category order by sum(total_amount) desc ) as rnk
    from fact_sales
    group by product_category, occupation
) t
where rnk <= 5
order by product_category, total_sales desc;


-- Q6
select City_Category, Marital_Status, YEAR(f.date) AS year, MONTH(f.date) AS month, SUM(total_amount) AS monthly_sales
from fact_sales f
join dateDim d ON f.dateid = d.dateid
where  d.year = 2020 and d.half_year = 'H2'
group by City_Category, Marital_Status, YEAR(f.date), MONTH(f.date)
order by City_Category, Marital_Status, MONTH(f.date);

-- Q7
select Stay_In_Current_City_Years AS stay_years, Gender, AVG(total_amount) AS avg_purchase
from fact_sales
group by Stay_In_Current_City_Years, Gender
order by stay_years, Gender, AVG(total_amount) desc;


-- Q8
select city_category, product_category, revenue
from (
    select city_category, product_category, sum(total_amount) as revenue,
        row_number() over ( partition by product_category order by sum(total_amount) desc) as rnk
    from fact_sales
    group by city_category, product_category
)t
where rnk <= 5
order by product_category, revenue desc;


-- Q9

select  StoreName, d.quarter, sum(total_amount) as quarterly_sales,
    (sum(total_amount) - lag(sum(total_amount)) over (partition by StoreName order by d.quarter)) /
    lag(sum(total_amount)) over (partition by StoreName order by d.quarter) * 100 as growth_rate
from fact_sales f
join dateDim d on f.dateid = d.dateid
where d.year = 2020
group by StoreName, d.quarter
order by StoreName, d.quarter;


-- Q10

select Age,
    case when d.weekend_flag = 1 then 'Weekend' else 'Weekday' end as day,
    SUM(total_amount) AS revenue
from fact_sales f
join dateDim d ON f.dateid = d.dateid
where d.year = 2020
group by Age, d.weekend_flag
order by Age, day;


-- Q11

select * from (
    select Product_Category,
        case when d.weekend_flag = 1 then 'Weekend' else 'Weekday' end as day_type, MONTH(f.date) as month,
        SUM(total_amount) as revenue,
        row_number() over ( partition by MONTH(f.date), case when d.weekend_flag = 1 THEN 'Weekend' ELSE 'Weekday' END
            order by SUM(total_amount) desc ) AS rnk
    from fact_sales f
    join dateDim d on f.dateid = d.dateid
    group by Product_Category, MONTH(f.date), day_type
)t 
where rnk <= 5
order by month, day_type, revenue DESC;




-- Q12

select  StoreName, d.quarter, SUM(total_amount) as quarterly_sales,
    (SUM(total_amount) - LAG(SUM(total_amount)) over (partition by StoreName order by d.quarter)) /
    LAG(SUM(total_amount)) over (partition by StoreName order by d.quarter) * 100 AS growth_rate
from fact_sales f
join dateDim d on f.dateid = d.dateid
where d.year = 2017
group by StoreName, d.quarter
order by StoreName, d.quarter;



-- Q13
select StoreName, SupplierName, Product_Category, SUM(total_amount) as total_sales
from fact_sales
group by StoreName, SupplierName, Product_Category
order by StoreName, SupplierName, total_sales desc;


-- Q14
select  season, Product_Category, SUM(total_amount) as total_sales
from fact_sales f
join dateDim d on f.dateid = d.dateid
group by season, Product_Category
order by season, total_sales desc;


-- Q15

select StoreName, SupplierName, MONTH(f.date) AS month, SUM(total_amount) AS monthly_sales,
    (SUM(total_amount) - LAG(SUM(total_amount)) over (partition by StoreName, SupplierName order by MONTH(f.date))) /
    LAG(SUM(total_amount)) over (partition by StoreName, SupplierName order by MONTH(f.date)) * 100 AS growth
from fact_sales f
join dateDim d on f.dateid = d.dateid
group by StoreName, SupplierName, MONTH(f.date)
order by StoreName, SupplierName, MONTH(f.date);


-- Q16
select p1.Product_Category AS Product1,  p2.Product_Category AS Product2,  COUNT(*) AS frequency
from fact_sales p1
join fact_sales p2 on p1.orderid = p2.orderid and p1.Product_Category < p2.Product_Category  -- avoids duplicate pairs
group by  Product1, Product2
order by frequency DESC
limit 5;



-- Q17

select StoreName, SupplierName, Product_Category, SUM(total_amount) as yearly_sales
from fact_sales f
join dateDim d on f.dateid = d.dateid
group by StoreName, SupplierName, Product_Category, d.year WITH ROLLUP
order by StoreName, SupplierName, Product_Category;


-- Q18

select Product_Category, d.half_year, d.year, SUM(total_amount) as total_revenue, SUM(quantity) as total_quantity
from fact_sales f
join dateDim d on f.dateid = d.dateid
group by Product_Category, d.year, d.half_year WITH ROLLUP
order by Product_Category, d.year, d.half_year desc;


-- Q19

select Product_Category, date, SUM(total_amount) AS daily_sales, AVG(SUM(total_amount)) over (partition by Product_Category) as avg_daily_sales,
    case when SUM(total_amount) > 2 * AVG(SUM(total_amount)) over (partition by Product_Category) then 'Spike' else 'Normal' end as flag
from fact_sales
group by Product_Category, date
order by Product_Category, date;


-- Q20

create view STORE_QUAsweRTERLY_SALES  as
select StoreName, d.quarter, SUM(total_amount) as quarterly_sales
from fact_sales f
join dateDim d on f.dateid = d.dateid
group by StoreName, d.quarter
order by StoreName, d.quarter;

select * from STORE_QUAsweRTERLY_SALES