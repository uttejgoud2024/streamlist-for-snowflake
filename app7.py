import streamlit as st
import sqlparse
import subprocess
import re
import os
import uuid

# --- SQL Validation ---
def validate_sql(sql_text):
    try:
        parsed = sqlparse.parse(sql_text)
        if not parsed or len(parsed) == 0:
            return False, "Empty or invalid SQL."
        return True, "SQL syntax looks valid."
    except Exception as e:
        return False, str(e)

# --- Strip DDL Statements ---
def strip_ddl(sql_text):
    sql_text = re.sub(r'(?i)CREATE\s+(OR\s+REPLACE\s+)?(VIEW|TABLE|PROCEDURE|FUNCTION)\s+[^\n]+\n?', '', sql_text)
    sql_text = re.sub(r'(?i)^AS\s*\n?', '', sql_text)
    return sql_text.strip()

# --- Oracle to Snowflake SQL Conversion ---
def convert_oracle_to_snowflake(sql_text):
    sql_text = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bNVL\s*\(([^,]+),\s*([^)]+)\)', r'COALESCE(\1, \2)', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bDECODE\s*\(([^,]+),\s*([^,]+),\s*([^,]+),\s*([^)]+)\)', r'CASE WHEN \1 = \2 THEN \3 ELSE \4 END', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bTO_DATE\s*\(([^)]+)\)', r'\1::DATE', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bTO_CHAR\s*\(([^)]+)\)', r'\1::TEXT', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bTO_NUMBER\s*\(([^)]+)\)', r'CAST(\1 AS NUMBER)', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bSUBSTR\s*\(([^,]+),\s*([^,]+)(?:,\s*([^)]+))?\)', r'SUBSTRING(\1, \2, \3)', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\(\+\)', '', sql_text)
    sql_text = re.sub(r'\bROWNUM\s*<=\s*(\d+)', r'LIMIT \1', sql_text, flags=re.IGNORECASE)
    return sql_text

# --- Wrap SQL in DBT Model ---
def wrap_sql_in_dbt_model(sql_text, model_type):
    config = f"{{{{ config(materialized='{model_type}') }}}}"
    return f"{config}\n\n{sql_text}"

# --- Run DBT Command ---
def run_dbt_command(command_list):
    try:
        result = subprocess.run(command_list, capture_output=True, text=True)
        return result.stdout, result.stderr
    except Exception as e:
        return "", str(e)

# --- Streamlit Tabs ---
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

with tab3:
    st.markdown("## 📁 Migration Settings")

    source_type = st.radio("Select Source File Type", ["Oracle SQL", "Excel"])
    target_type = st.radio("Select Target File Type", ["DBT Model (SQL)", "Snowflake SQL"])
    model_type = st.selectbox("Select DBT Model Type", ["view", "table"])

    st.markdown("### 📂 Upload Oracle SQL Files")
    uploaded_files = st.file_uploader(
        "Drag and drop Oracle SQL files here",
        type=["sql"],
        accept_multiple_files=True
    )

    output_dir = None
    if dbt_path:
        output_dir = os.path.join(dbt_path, "models", subfolder)
        os.makedirs(output_dir, exist_ok=True)
    else:
        st.warning("⚠️ Please provide a valid DBT project path to save models.")

    st.markdown("### 🧪 DBT Command")
    run = st.checkbox("Run")
    test = st.checkbox("Test")
    dbt_command = "run" if run else "test" if test else ""

    if st.button("🚀 Convert and Save Models"):
        if uploaded_files and output_dir:
            with st.spinner("Converting SQL files..."):
                for file in uploaded_files:
                    try:
                        sql_content = file.read().decode("utf-8")
                        if re.search(r'\bCREATE\s+(PROCEDURE|FUNCTION)\b', sql_content, re.IGNORECASE):
                            st.warning(f"⚠️ `{file.name}` contains a procedure/function which is not supported in DBT models.")
                        sql_content = strip_ddl(sql_content)
                        is_valid, message = validate_sql(sql_content)
                        if not is_valid:
                            st.error(f"Validation failed for `{file.name}`: {message}")
                            continue
                        converted_sql = convert_oracle_to_snowflake(sql_content)
                        wrapped_sql = wrap_sql_in_dbt_model(converted_sql, model_type)
                        base_name = os.path.splitext(file.name)[0]
                        safe_name = re.sub(r'[^a-zA-Z0-9_\-]', '_', base_name)
                        output_filename = os.path.join(output_dir, f"{safe_name}_{uuid.uuid4().hex[:8]}.sql")
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
        st.session_state["dbt_path"] = dbt_path
        if dbt_path and dbt_command:
            if not os.path.exists(dbt_path):
                st.error("❌ The specified DBT project path does not exist.")
            else:
                cmd_list = ["dbt", dbt_command, "--project-dir", dbt_path]
                st.markdown(f"### Running: `{' '.join(cmd_list)}`")
                with st.spinner("Executing DBT..."):
                    stdout, stderr = run_dbt_command(cmd_list)
                st.text_area("📄 DBT Output", stdout, height=200)
                if stderr:
                    st.text_area("❌ DBT Errors", stderr, height=200)
        else:
            st.warning("⚠️ Please provide DBT project path and select a command.")