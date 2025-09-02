import streamlit as st
import sqlparse
import subprocess
import re
import os
import uuid
import shlex

# --- SQL Validation ---
def validate_sql(sql_text):
    """Validates SQL syntax and checks for DDL statements."""
    try:
        if not sql_text.strip():
            return False, "Empty or invalid SQL."
        
        # Check for unsupported DDL statements
        if re.search(r'\b(CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE)\b', sql_text, re.IGNORECASE):
            return False, "DDL statements (CREATE, ALTER, DROP, etc.) are not allowed in this utility. Please use pure DML (SELECT, INSERT, UPDATE) queries."
        
        parsed = sqlparse.parse(sql_text)
        if not parsed or len(parsed) == 0:
            return False, "Empty or invalid SQL."

        return True, "SQL syntax looks valid."
    except Exception as e:
        return False, f"SQL syntax error: {str(e)}"

# --- Strip DDL Statements ---
def strip_ddl(sql_text):
    """
    Strips CREATE, ALTER, and DROP statements from the SQL text.
    This is a fallback in case the validation is bypassed.
    """
    # Using a more comprehensive regex to catch more DDL types
    sql_text = re.sub(r'(?i)(CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE)\s+(OR\s+REPLACE\s+)?(VIEW|TABLE|PROCEDURE|FUNCTION|INDEX|SEQUENCE)\s+[^\n]+\n?', '', sql_text)
    sql_text = re.sub(r'(?i)^AS\s*\n?', '', sql_text)
    return sql_text.strip()

# --- Oracle to Snowflake SQL Conversion ---
def convert_oracle_to_snowflake(sql_text):
    """
    Converts common Oracle functions to their Snowflake equivalents.
    This function has been expanded for better coverage.
    """
    sql_text = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bNVL\s*\(([^,]+),\s*([^)]+)\)', r'COALESCE(\1, \2)', sql_text, flags=re.IGNORECASE)
    
    # Improved DECODE conversion to handle multiple arguments
    def decode_to_case(match):
        args = [arg.strip() for arg in match.group(1).split(',')]
        if len(args) < 3:
            return match.group(0) # Not a valid DECODE, return original
        
        case_statement = f"CASE {args[0]} "
        # Iterate over pairs of arguments (starting from the second)
        for i in range(1, len(args) - 1, 2):
            case_statement += f"WHEN {args[i]} THEN {args[i+1]} "
        
        # Add the ELSE clause if there's an odd number of arguments (meaning a default value)
        if len(args) % 2 == 0:
            case_statement += f"ELSE {args[-1]} "
            
        case_statement += "END"
        return case_statement

    sql_text = re.sub(r'\bDECODE\s*\(([^)]+)\)', decode_to_case, sql_text, flags=re.IGNORECASE)

    sql_text = re.sub(r'\bTO_DATE\s*\(([^,]+),\s*([^)]+)\)', r"TO_DATE(\1, \2)", sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bTO_CHAR\s*\(([^,]+),\s*([^)]+)\)', r"TO_VARCHAR(\1, \2)", sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bTO_NUMBER\s*\(([^)]+)\)', r'TRY_TO_NUMBER(\1)', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bSUBSTR\s*\(([^,]+),\s*([^,]+)(?:,\s*([^)]+))?\)', r'SUBSTRING(\1, \2, \3)', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\(\+\)', '', sql_text)
    sql_text = re.sub(r'\bROWNUM\s*<=\s*(\d+)', r'LIMIT \1', sql_text, flags=re.IGNORECASE)
    return sql_text

# --- Wrap SQL in DBT Model ---
def wrap_sql_in_dbt_model(sql_text, model_type):
    """Wraps the SQL in a DBT config block."""
    config = f"{{{{ config(materialized='{model_type}') }}}}"
    return f"{config}\n\n{sql_text}"

# --- Run DBT Command ---
def run_dbt_command(command_list, project_dir):
    """
    Runs a DBT command safely by using shlex to prevent shell injection.
    """
    try:
        # Use shlex.split to safely handle the command list
        command_list = ["dbt"] + shlex.split(" ".join(command_list))
        result = subprocess.run(command_list, cwd=project_dir, capture_output=True, text=True, check=True)
        return result.stdout, result.stderr
    except subprocess.CalledProcessError as e:
        return e.stdout, e.stderr
    except Exception as e:
        return "", str(e)

# --- Streamlit UI ---
st.set_page_config(page_title="Oracle to Snowflake DBT Migration", layout="wide")
tab1, tab2, tab3 = st.tabs(["🏠 Home", "⚙️ Environment Setup", "📁 Migration Settings"])

with tab1:
    st.markdown("<h1 style='text-align: center; color: #2E86C1;'>🚀 Oracle to Snowflake DBT Migration</h1>", unsafe_allow_html=True)
    st.markdown("### Introduction")
    st.markdown("**A Python-powered Streamlit app that helps migrate Oracle SQL queries to Snowflake DBT models—complete with validation, conversion, and documentation.**")
    st.markdown("#### Features")
    st.markdown("- ✅ SQL Validation: Ensures your Oracle SQL syntax is correct before conversion.")
    st.markdown("- 🔄 Oracle-to-Snowflake Conversion: Automatically translates Oracle-specific functions to Snowflake-compatible syntax.")
    st.markdown("- 📦 Bulk Migration: Upload and process multiple SQL files in one go.")
    st.markdown("- 🧱 DBT Model Wrapping: Wraps converted SQL into DBT-compatible models (view or table).")
    st.markdown("- 📄 SQL File Generation: Saves converted SQL as downloadable DBT model files.")
    st.markdown("- 🚀 Run DBT Commands: Execute DBT `run` or `test` directly from the app.")

with tab2:
    st.markdown("## ⚙️ Environment Setup")
    st.markdown("### 🛠️ Pre-Requisites")
    st.markdown("Please ensure the following are installed on your system **before proceeding**:")
    st.code("python version: >=3.9 <=3.12")
    st.code("pip install snowflake-connector-python")
    st.code("pip install streamlit")
    st.code("pip install dbt-core==1.9.4 dbt-snowflake==1.9.4")
    st.subheader("For running the utility install below packages:")
    st.code("pip install sqlparse")
    st.code("pip install pandas")

    dbt_path = st.text_input("DBT Project Path", value=st.session_state.get("dbt_path", ""))
    subfolder = st.text_input("Subfolder inside models (optional)", value="oracle_migration")
    
    st.session_state["dbt_path"] = dbt_path
    st.session_state["subfolder"] = subfolder

with tab3:
    st.markdown("## 📁 Migration Settings")
    
    col1, col2 = st.columns(2)
    with col1:
        source_type = st.radio("Select Source File Type", ["Oracle SQL", "Excel"])
    with col2:
        target_type = st.radio("Select Target File Type", ["DBT Model (SQL)", "Snowflake SQL"])

    model_type = st.selectbox("Select DBT Model Type", ["view", "table"])
    
    st.markdown("### 📂 Upload Oracle SQL Files")
    uploaded_files = st.file_uploader(
        "Drag and drop Oracle SQL files here",
        type=["sql"],
        accept_multiple_files=True
    )

    dbt_path = st.session_state.get("dbt_path")
    subfolder = st.session_state.get("subfolder")
    
    output_dir = None
    if dbt_path:
        output_dir = os.path.join(dbt_path, "models", subfolder)
        os.makedirs(output_dir, exist_ok=True)
    else:
        st.warning("⚠️ Please provide a valid DBT project path to save models.")

    st.markdown("### 🧪 DBT Command")
    run = st.checkbox("Run")
    test = st.checkbox("Test")
    
    if st.button("🚀 Convert and Save Models"):
        if uploaded_files and output_dir:
            with st.spinner("Converting SQL files..."):
                for file in uploaded_files:
                    try:
                        sql_content = file.read().decode("utf-8")
                        
                        # Use a more explicit check for non-SELECT statements
                        parsed_statements = sqlparse.parse(sql_content)
                        is_select = True
                        for stmt in parsed_statements:
                            if not stmt.get_type() == 'SELECT':
                                is_select = False
                                st.warning(f"⚠️ `{file.name}` contains non-SELECT statements which may not be compatible with DBT models.")
                                break
                        
                        is_valid, message = validate_sql(sql_content)
                        if not is_valid:
                            st.error(f"Validation failed for `{file.name}`: {message}")
                            continue
                            
                        # Strip any DDL just in case the validation was bypassed
                        cleaned_sql = strip_ddl(sql_content)
                        
                        converted_sql = convert_oracle_to_snowflake(cleaned_sql)
                        wrapped_sql = wrap_sql_in_dbt_model(converted_sql, model_type)
                        
                        base_name = os.path.splitext(file.name)[0]
                        safe_name = re.sub(r'[^a-zA-Z0-9_.]', '_', base_name)
                        
                        # Create a clean filename without a random UUID for better readability
                        output_filename = os.path.join(output_dir, f"{safe_name}.sql")
                        
                        with open(output_filename, "w") as f:
                            f.write(wrapped_sql)
                            
                        st.markdown(f"### ✅ Converted SQL for `{file.name}`")
                        st.code(wrapped_sql, language="sql")
                        
                        with open(output_filename, "rb") as f:
                            st.download_button(label=f"⬇️ Download `{safe_name}.sql`", data=f, file_name=f"{safe_name}.sql", mime="text/sql")
                        
                        st.success(f"✅ Saved to `{output_filename}`")
                    except Exception as e:
                        st.error(f"❌ Error processing `{file.name}`: {str(e)}")
        else:
            st.warning("⚠️ Please upload at least one SQL file and provide a valid DBT path.")

    if st.button("▶️ Execute DBT"):
        dbt_command_list = []
        if run:
            dbt_command_list.append("run")
        if test:
            dbt_command_list.append("test")

        if dbt_path and dbt_command_list:
            if not os.path.exists(dbt_path) or not os.path.exists(os.path.join(dbt_path, "dbt_project.yml")):
                st.error("❌ The specified path is not a valid DBT project directory.")
            else:
                st.markdown(f"### Running: `dbt {' '.join(dbt_command_list)}`")
                with st.spinner("Executing DBT..."):
                    stdout, stderr = run_dbt_command(dbt_command_list, dbt_path)
                st.text_area("📄 DBT Output", stdout, height=200)
                if stderr:
                    st.text_area("❌ DBT Errors", stderr, height=200)
        else:
            st.warning("⚠️ Please provide DBT project path and select at least one command (Run or Test).")
