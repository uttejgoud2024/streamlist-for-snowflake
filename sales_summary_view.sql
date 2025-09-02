CREATE OR REPLACE VIEW sales_summary_view AS
  SELECT
    o.order_id,
    o.order_date,
    c.customer_name,
    p.product_name,
    o.quantity * p.price AS total_sale,
    ROWNUM AS rn
  FROM orders o, customers c, products p
  WHERE o.customer_id = c.customer_id(+)
    AND o.product_id = p.product_id
    AND ROWNUM <= 100;
