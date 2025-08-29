import streamlit as st
import sqlparse
import subprocess
import re
import os
import uuid
import yaml
import glob

# --- SQL Validation ---
def validate_sql(sql_text):
    try:
        parsed = sqlparse.parse(sql_text)
        if not parsed or len(parsed) == 0:
            return False, "Empty or invalid SQL."
        return True, "SQL syntax looks valid."
    except Exception as e:
        return False, str(e)
    
# --- Function to parse dbt model YAML files ---
def parse_dbt_yml(file_path):
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
    docs = []
    for model in data.get('models', []):
        model_name = model.get('name', 'Unnamed Model')
        description = model.get('description', '')
        columns = model.get('columns', [])
        column_docs = "\n".join([f"- **{col['name']}**: {col.get('description', '')}" for col in columns])
        docs.append(f"### {model_name}\n\n**Description**: {description}\n\n**Columns**:\n{column_docs}\n")
    return docs

# --- Oracle to Snowflake SQL Conversion ---
def convert_oracle_to_snowflake(sql_text):
    sql_text = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bNVL\s*\(([^,]+),\s*([^)]+)\)', r'COALESCE(\1, \2)', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bDECODE\s*\(\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*([^)]+)\)', r'CASE WHEN \1 = \2 THEN \3 ELSE \4 END', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bTO_DATE\s*\(\s*([^)]+)\)', r'\1::DATE', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bTO_CHAR\s*\(\s*([^)]+)\)', r'\1::TEXT', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\(\+\)', '', sql_text)
    sql_text = re.sub(r'\bROWNUM\s*<=\s*(\d+)', r'LIMIT \1', sql_text, flags=re.IGNORECASE)
    return sql_text

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
-- Add incremental filter logic here
{{% endif %}}"""
    else:
        return sql_text

# --- Run DBT Command ---
def run_dbt_command(command):
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
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
        if dbt_path:
            cmd = f"dbt {dbt_command} --project-dir {dbt_path}"
            st.markdown(f"### Running: `{cmd}`")
            stdout, stderr = run_dbt_command(cmd)
            st.text_area("DBT Output", stdout, height=200)
            if stderr:
                st.text_area("DBT Errors", stderr, height=200)
        else:
            st.warning("Please provide DBT project path.")
    
    st.markdown("### SQL Validation")
    sql_input = st.text_area("Paste your Oracle SQL query here")
    if st.button("Validate SQL"):
        is_valid, message = validate_sql(sql_input)
        if is_valid:
            st.success(message)
        else:
            st.error(f"Validation failed: {message}")

    st.markdown("### DBT Model Documentation Generator")

    model_dir = st.text_input("Enter path to your dbt models directory", "./models")
    
    if model_dir and os.path.isdir(model_dir):
        yml_files = glob.glob(os.path.join(model_dir, "**/*.yml"), recursive=True)
        yaml_files = glob.glob(os.path.join(model_dir, "**/*.yaml"), recursive=True)
        all_files = yml_files + yaml_files
        if all_files:
            st.success(f"Found {len(all_files)} YAML files.")
            all_docs = []
            for all_files in all_files:
                docs = parse_dbt_yml(all_files)
                all_docs.extend(docs)

            st.markdown("### Generated Documentation")
            for doc in all_docs:
                st.markdown(doc)

            if st.button("Export as Markdown"):
                with open("dbt_docs.md", "w") as f:
                    f.write("\n\n".join(all_docs))
                st.success("Documentation exported to `dbt_docs.md`.")
        else:
            st.warning("No YAML files found in the specified directory.")
    
    else:
        st.info("Please enter a valid directory path.")


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