CREATE TABLE product_cor AS
SELECT *
FROM (
    SELECT *
        ,ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY list_price DESC) AS rn
    FROM product
) AS subquery
WHERE rn = 1;