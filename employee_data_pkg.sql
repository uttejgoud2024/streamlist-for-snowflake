CREATE OR REPLACE PACKAGE employee_data_pkg AS
  PROCEDURE process_new_hires;
  FUNCTION get_department_name(p_dept_id IN NUMBER) RETURN VARCHAR2;
END employee_data_pkg;
/

CREATE OR REPLACE PACKAGE BODY employee_data_pkg AS
  FUNCTION get_department_name(p_dept_id IN NUMBER) RETURN VARCHAR2 AS
    v_dept_name VARCHAR2(100);
  BEGIN
    SELECT department_name INTO v_dept_name FROM departments WHERE department_id = p_dept_id;
    RETURN v_dept_name;
  END get_department_name;

  PROCEDURE process_new_hires AS
    CURSOR new_hires_cursor IS
      SELECT employee_id, employee_name, department_id, start_date
      FROM employees WHERE start_date > SYSDATE - 30;
  BEGIN
    FOR rec IN new_hires_cursor LOOP
      INSERT INTO employee_logs (log_id, employee_id, message)
      VALUES (SYS.GUID(), rec.employee_id, 'New hire processed for ' || get_department_name(rec.department_id));
    END LOOP;
  END process_new_hires;
END employee_data_pkg;
/
