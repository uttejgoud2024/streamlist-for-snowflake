import streamlit as st
import sqlparse
import subprocess
import os
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
selected_page = st.sidebar.radio(
    "", pages, index=st.session_state.page_index, key="sidebar_radio"
)

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
    - 🔄 Auto-conversion (placeholder)  
    - 📦 Bulk Migration  
    - 📘 Documentation Generator  
    - 🚀 Run DBT Commands (in Environment Setup)
    """)

elif current_page == "Environment Setup":
    st.markdown("###  Pre-Requisites")
    st.markdown("Please ensure the following are installed on your system **before proceeding**:")
    st.code("python version: >=9;<=12;")
    st.code("pip install snowflake")
    st.code("pip install streamlit")
    st.code("pip install dbt-core==1.9.4 dbt-snowflake==1.9.4")
    st.subheader("For running the utility install below packages:")
    st.code("pip install sqlparse")
    st.code("pip install pandas")

elif current_page == "Migration Settings":
    uploaded_files = st.file_uploader("Upload Oracle SQL Files", accept_multiple_files=True, type=["sql"])
    model_type = st.selectbox("Select DBT Model Type", ["view", "table", "incremental"])

    if st.button("Convert to DBT Models"):
        if uploaded_files:
            for file in uploaded_files:
                sql_content = file.read().decode("utf-8")
                st.markdown(f"### Converted SQL for `{file.name}`")
                # Placeholder: Add real Oracle to Snowflake SQL conversion logic here
                converted_sql = f"-- Converted to {model_type}\n{sql_content}"  # Replace with actual conversion logic
                st.code(converted_sql, language="sql")
            st.success("Conversion completed!")
        else:
            st.warning("Please upload at least one SQL file.")

    st.markdown("### DBT Project")
    dbt_path = st.text_input("DBT Project Path", value=st.session_state.get("dbt_path", ""))
    dbt_command = st.selectbox(
        "Select DBT Command",
        ["run", "build", "test", "seed", "snapshot", "docs generate"]
    )

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
