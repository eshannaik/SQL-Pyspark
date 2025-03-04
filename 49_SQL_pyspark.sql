-- CREATE TABLE sf_exchange_rate_GS_49 ( date DATE, exchange_rate FLOAT, source_currency VARCHAR(10), target_currency VARCHAR(10));

-- INSERT INTO sf_exchange_rate_GS_49 (date, exchange_rate, source_currency, target_currency) VALUES ('2020-01-15', 1.1, 'EUR', 'USD'), ('2020-01-15', 1.3, 'GBP', 'USD'), ('2020-02-05', 1.2, 'EUR', 'USD'), 
-- ('2020-02-05', 1.35, 'GBP', 'USD'), ('2020-03-25', 1.15, 'EUR', 'USD'), ('2020-03-25', 1.4, 'GBP', 'USD'), ('2020-04-15', 1.2, 'EUR', 'USD'), ('2020-04-15', 1.45, 'GBP', 'USD'), ('2020-05-10', 1.1, 'EUR', 'USD'), 
-- ('2020-05-10', 1.3, 'GBP', 'USD'), ('2020-06-05', 1.05, 'EUR', 'USD'), ('2020-06-05', 1.25, 'GBP', 'USD');

-- CREATE TABLE sf_sales_amount_GS_49 ( currency VARCHAR(10), sales_amount BIGINT, sales_date DATE);

-- INSERT INTO sf_sales_amount_GS_49 (currency, sales_amount, sales_date) VALUES ('USD', 1000, '2020-01-15'), ('EUR', 2000, '2020-01-20'), ('GBP', 1500, '2020-02-05'), ('USD', 2500, '2020-02-10'), 
-- ('EUR', 1800, '2020-03-25'), ('GBP', 2200, '2020-03-30'), ('USD', 3000, '2020-04-15'), ('EUR', 1700, '2020-04-20'), ('GBP', 2000, '2020-05-10'), ('USD', 3500, '2020-05-25'), ('EUR', 1900, '2020-06-05'), ('GBP', 2100, '2020-06-10');


SELECT DISTINCT quarter(sales_date) as 'Quarter',ROUND(SUM((sales_amount*exchange_rate)),2) as 'sum'
FROM sf_exchange_rate_GS_49 e
JOIN sf_sales_amount_GS_49 s
ON e.date = s.sales_date AND s.currency=e.source_currency 
WHERE quarter(sales_date)<=2 and year(sales_date)=2020
group by quarter(sales_date);


from pyspark.sql import functions as F
from pyspark.sql.functions import quarter, year, sum as spark_sum, round as spark_round

sf_exchange_rate_GS_49.join(sf_sales_amount_GS_49,(sf_exchange_rate_GS_49["date"] == sf_sales_amount_GS_49["sales_date"]) & (sf_sales_amount_GS_49["currency"]==sf_exchange_rate_GS_49["source_currency"]) ,"inner")\
.withColumn("Quarter",quarter(F.col("sales_date")))
.groupBy(F.col("sales_date"))\
.agg(spark_round(spark_sum(F.col("sales_amount")*F.col("exchange_rate")),2).alias("sum"))
.filter(F.col("Quarter")<=2 and year(F.col("sales_date"))==2020)
.select("Quarter","sum")
.show()



# Add Quarter column and calculate the sum of sales in local currency
sf_transformed = sf_joined.withColumn("Quarter", quarter(F.col("sales_date")))\
    .groupBy("Quarter")\
    .agg(spark_round(spark_sum(F.col("sales_amount") * F.col("exchange_rate")), 2).alias("sum"))

# Apply filter for year 2020 and quarters <= 2
sf_filtered = sf_transformed.filter((F.col("Quarter") <= 2) & (year(F.col("sales_date")) == 2020))

# Select required columns and display the result
sf_filtered.select("Quarter", "sum").show()
