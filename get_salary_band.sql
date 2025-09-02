CREATE OR REPLACE FUNCTION get_salary_band(p_salary IN NUMBER)
RETURN VARCHAR2 AS
  v_band VARCHAR2(20);
BEGIN
  v_band := CASE
              WHEN p_salary > 150000 THEN 'Senior Executive'
              WHEN p_salary > 100000 THEN 'Manager'
              WHEN p_salary > 50000 THEN 'Team Lead'
              ELSE 'Staff'
            END;
  RETURN v_band;
END;
/
