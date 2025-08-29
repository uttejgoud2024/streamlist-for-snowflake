import streamlit as st
import sqlparse
import subprocess
import re
import os
import uuid
import sqlglot

# --- SQL Validation ---
def validate_sql(sql_text):
    try:
        parsed = sqlparse.parse(sql_text)
        if not parsed or len(parsed) == 0:
            return False, "Empty or invalid SQL."
        return True, "SQL syntax looks valid."
    except Exception as e:
        return False, str(e)

# --- Oracle to Snowflake SQL Conversion ---
def convert_oracle_to_snowflake(sql_text, options=None):
    options = options or {}
    # Flags for selective conversions
    c = lambda k: options.get(k, True)

    if c('sysdate'):
        re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bNVL\s*\(([^,]+),\s*([^)]+)\)', r'COALESCE(\1, \2)', sql_text, flags=re.IGNORECASE) if c('nvl') else sql_text
    sql_text = re.sub(r'\bDECODE\s*\(\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*([^)]+)\)', r'CASE WHEN \1 = \2 THEN \3 ELSE \4 END', sql_text, flags=re.IGNORECASE) if c('decode') else sql_text
    sql_text = re.sub(r'\bTO_DATE\s*\(\s*([^)]+)\)', r'\1::DATE', sql_text, flags=re.IGNORECASE) if c('to_date') else sql_text
    sql_text = re.sub(r'\bTO_CHAR\s*\(\s*([^)]+)\)', r'\1::TEXT', sql_text, flags=re.IGNORECASE) if c('to_char') else sql_text
    sql_text = re.sub(r'\(\+\)', '', sql_text) if c('oracle_outer_join') else sql_text
    sql_text = re.sub(r'\bROWNUM\s*<=\s*(\d+)', r'LIMIT \1', sql_text, flags=re.IGNORECASE) if c('rownum_limit') else sql_text
    return sql_text


# --- Oracle to Snowflake Conversion via sqlglot ---
def convert_with_sqlglot(sql_text):
    try:
        result = sqlglot.transpile(sql_text, read="oracle", write="snowflake")
        if result:
            return result[0], None
        return None, "sqlglot did not return a result"
    except Exception as e:
        return None, str(e)

# --- Wrap SQL in DBT Model ---
def wrap_sql_in_dbt_model(sql_text, model_type, unique_key="id"):
    if model_type == "view":
        return f"{{{{ config(materialized='view') }}}}\n\n{sql_text}"
    elif model_type == "table":
        return f"{{{{ config(materialized='table') }}}}\n\n{sql_text}"
    elif model_type == "incremental":
        return f"""{{{{ config(materialized='incremental', unique_key='{unique_key}', tags=['oracle_migration']) }}}}
{sql_text}

{{% if is_incremental() %}}
-- Example: Only include new records
-- WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{{% endif %}}"""
    else:
        return sql_text

# --- Run DBT Command ---
def run_dbt_command(command):
    try:
        result = subprocess.run(command.split(), capture_output=True, text=True)
        return result.stdout, result.stderr
    except Exception as e:
        return "", str(e)

# --- Page Navigation Setup ---
pages = ["Home", "Environment Setup", "Migration Settings"]

if "page_index" not in st.session_state:
    st.session_state.page_index = 0

# Sidebar navigation
st.sidebar.title("Oracle ➜ Snowflake DBT Migration")
selected_page = st.sidebar.radio("", pages, index=st.session_state.page_index, key="sidebar_radio")

# Sync page_index with sidebar selection
if pages.index(selected_page) != st.session_state.page_index:
    st.session_state.page_index = pages.index(selected_page)
    st.rerun()

# --- Page Content ---
current_page = pages[st.session_state.page_index]

st.markdown("## Oracle to Snowflake DBT Migration")

if current_page == "Home":
    st.markdown("### 🗺️ Introduction")
    st.markdown("""
    A Python-powered Streamlit app that helps migrate Oracle SQL queries to Snowflake DBT models—complete with validation, conversion, and documentation.
    """)
    st.markdown("### ✨ Features")
    st.markdown("""
    - ✅ SQL Validation  
    - 🔄 Oracle-to-Snowflake Conversion  
    - 📦 Bulk Migration  
    - 🧱 DBT Model Wrapping  
    - 📄 SQL File Generation  
    - 🚀 Run DBT Commands
    """)

elif current_page == "Environment Setup":
    st.markdown("###  Pre-Requisites")
    st.markdown("Please ensure the following are installed on your system **before proceeding**:")
    st.code("python version: >=3.9;<=3.12")
    st.code("pip install snowflake-connector-python")
    st.code("pip install streamlit")
    st.code("pip install dbt-core==1.9.4 dbt-snowflake==1.9.4")
    st.subheader("For running the utility install below packages:")
    st.code("pip install sqlparse")
    st.code("pip install pandas")

elif current_page == "Migration Settings":
    uploaded_files = st.file_uploader("Upload Oracle SQL Files", accept_multiple_files=True, type=["sql"])
    model_type = st.selectbox("Select DBT Model Type", ["view", "table", "incremental"])
    unique_key = ""
    if model_type == "incremental":
        unique_key = st.text_input("Enter unique_key for incremental model", value="id")

    output_dir = "converted_models"
    os.makedirs(output_dir, exist_ok=True)

    if st.button("Convert and Save Models"):
        if uploaded_files:
            for file in uploaded_files:
                try:
                    sql_content = file.read().decode("utf-8")
                    is_valid, message = validate_sql(sql_content)
                    if not is_valid:
                        st.error(f"Validation failed for `{file.name}`: {message}")
                        continue

                    converted_sql = convert_oracle_to_snowflake(sql_content)
                    wrapped_sql = wrap_sql_in_dbt_model(converted_sql, model_type, unique_key)

                    # Sanitize filename
                    base_name = os.path.splitext(file.name)[0]
                    safe_name = re.sub(r'[^a-zA-Z0-9_\-]', '_', base_name)
                    output_filename = os.path.join(output_dir, f"{safe_name}_{uuid.uuid4().hex[:8]}.sql")

                    # Save to .sql file
                    try:
                        with open(output_filename, "w") as f:
                            f.write(wrapped_sql)
                        st.markdown(f"### Converted SQL for `{file.name}`")
                        st.code(wrapped_sql, language="sql")
                        with open(output_filename, "rb") as f:
                            st.download_button(label=f"Download `{safe_name}.sql`", data=f, file_name=f"{safe_name}.sql")
                        st.success(f"Saved to `{output_filename}`")
                    except Exception as e:
                        st.error(f"Failed to save `{file.name}`: {str(e)}")
                except Exception as e:
                    st.error(f"Error processing `{file.name}`: {str(e)}")
        else:
            st.warning("Please upload at least one SQL file.")

    st.markdown("### DBT Project")
    dbt_path = st.text_input("DBT Project Path", value=st.session_state.get("dbt_path", ""))
    dbt_command = st.selectbox("Select DBT Command", ["run", "build", "test", "seed", "snapshot", "docs generate"])

    if st.button("Execute DBT"):
        st.session_state["dbt_path"] = dbt_path

        # Auto-detect dbt_project.yml if path empty
        auto_path = ""
        if not dbt_path:
            for candidate in [os.getcwd(), os.path.join(os.getcwd(), "dbt"), os.path.join(os.getcwd(), "project")]:
                if os.path.exists(os.path.join(candidate, "dbt_project.yml")):
                    auto_path = candidate
                    break

        effective_path = dbt_path or auto_path
        if not effective_path:
            st.warning("Please provide DBT project path (dbt_project.yml not found automatically).")
        else:
            # Whitelisted commands mapping
            allowed_commands = {
                "run": ["dbt", "run", "--project-dir", effective_path],
                "build": ["dbt", "build", "--project-dir", effective_path],
                "test": ["dbt", "test", "--project-dir", effective_path],
                "seed": ["dbt", "seed", "--project-dir", effective_path],
                "snapshot": ["dbt", "snapshot", "--project-dir", effective_path],
                "docs generate": ["dbt", "docs", "generate", "--project-dir", effective_path],
            }
            cmd_list = allowed_commands.get(dbt_command)
            if not cmd_list:
                st.error("Unsupported DBT command selected.")
            else:
                st.markdown(f"### Running: `{' '.join(cmd_list)}`")
                stdout, stderr = run_dbt_command(' '.join(cmd_list))
                st.text_area("DBT Output", stdout, height=200)
                if stderr:
                    st.text_area("DBT Errors", stderr, height=200)

# --- Navigation Buttons ---
st.markdown("<br><hr>", unsafe_allow_html=True)
col1, col2 = st.columns([6, 1])
with col1:
    if st.button("⬅️ Previous") and st.session_state.page_index > 0:
        st.session_state.page_index -= 1
        st.rerun()
with col2:
    if st.button("Next ➡️") and st.session_state.page_index < len(pages) - 1:
        st.session_state.page_index += 1
        st.rerun()