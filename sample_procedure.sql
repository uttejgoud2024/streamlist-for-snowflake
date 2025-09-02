CREATE OR REPLACE PROCEDURE calculate_employee_bonuses (
    p_department_id IN NUMBER
)
AS
    v_total_sales NUMBER;
    v_bonus_percent NUMBER;

    CURSOR c_employees IS
        SELECT employee_id, employee_name, sales_ytd
        FROM employees
        WHERE department_id = p_department_id;

BEGIN
    -- Get total sales for the department, handling NULL values with NVL
    SELECT NVL(SUM(sales_ytd), 0)
    INTO v_total_sales
    FROM employees
    WHERE department_id = p_department_id;

    -- Determine bonus percentage based on total sales
    IF v_total_sales > 1000000 THEN
        v_bonus_percent := 0.10;
    ELSIF v_total_sales > 500000 THEN
        v_bonus_percent := 0.05;
    ELSE
        v_bonus_percent := 0.02;
    END IF;

    -- Loop through employees and insert bonus records
    FOR emp_rec IN c_employees LOOP
        INSERT INTO employee_bonuses (
            employee_id,
            bonus_amount,
            bonus_date
        ) VALUES (
            emp_rec.employee_id,
            emp_rec.sales_ytd * v_bonus_percent,
            SYSDATE
        );
    END LOOP;

    -- Commit the changes
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END calculate_employee_bonuses;
/
