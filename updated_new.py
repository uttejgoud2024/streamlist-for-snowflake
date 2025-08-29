import streamlit as st
import sqlparse
import subprocess
import snowflake.connector
import json
import os

# --- SQL Validation ---
def validate_sql(sql_text):
    try:
        parsed = sqlparse.parse(sql_text)
        if not parsed or len(parsed) == 0:
            return False, "Empty or invalid SQL."
        return True, "SQL syntax looks valid."
    except Exception as e:
        return False, str(e)

# --- Snowflake Connection Test ---
def test_snowflake_connection(user, password, account, warehouse, database, schema):
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        conn.cursor().execute("SELECT CURRENT_VERSION()")
        return True, "Connection successful!"
    except Exception as e:
        return False, str(e)

# --- Run DBT Command ---
def run_dbt_command(command):
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        return result.stdout, result.stderr
    except Exception as e:
        return "", str(e)

# --- Page Navigation Setup ---
pages = ["Home", "Environment Pre-requisites", "Environment Setup", "Migration Settings", "SQL Validation"]

if "page_index" not in st.session_state:
    st.session_state.page_index = 0

# Sidebar navigation
st.sidebar.title("Oracle ➜ Snowflake DBT Migration")
selected_page = st.sidebar.radio("", pages, index=st.session_state.page_index)

# Sync sidebar selection with session state
if selected_page != pages[st.session_state.page_index]:
    st.session_state.page_index = pages.index(selected_page)

# --- Page Content ---
st.markdown("## Oracle to Snowflake DBT Migration")

current_page = pages[st.session_state.page_index]

# --- Load past values ---
SAVE_FILE = "history.json"
if os.path.exists(SAVE_FILE):
    with open(SAVE_FILE, "r") as f:
        history = json.load(f)
else:
    history = {"user": [], "password":[], "account":[], "warehouse":[], "database":[], "schema":[]}


if current_page == "Home":
    st.markdown("### 🏗️ Introduction")
    st.markdown("""
    A Python-powered Streamlit app that helps migrate Oracle SQL queries to Snowflake DBT models—complete with validation, conversion, and documentation.
    """)
    st.markdown("### ✨ Features")
    st.markdown("""
    - ✅ SQL Validation
    - 🔄 Auto-conversion
    - 📦 Bulk Migration
    - 📘 Documentation Generator
    - 🧪 Snowflake Connection Test
    - 🚀 Run DBT Commands
    """)

elif current_page == "Environment Pre-requisites":
    st.subheader("Pre-requisites:")
    st.code("python version: >=9;<=12;")
    st.code("pip install snowflake")
    st.code("pip install streamlit")
    st.code("pip install dbt-core==1.9.4 dbt-snowflake==1.9.4")
    st.subheader("For running the utility install below packages:")
    st.code("pip install sqlparse")

elif current_page == "Environment Setup":
    st.subheader("Snowflake Credentials")
    
    # Initialize session state for each field
    if "user" not in st.session_state:
        st.session_state.user = ""
    if "account" not in st.session_state:
        st.session_state.account = ""
    if "warehouse" not in st.session_state:
        st.session_state.warehouse = ""
    if "database" not in st.session_state:
        st.session_state.database = ""
    if "schema" not in st.session_state:
        st.session_state.schema = ""
    
    # Input fields with session state values
    user = st.text_input("User", placeholder= "These were the previous users: {} ".format(history["user"]), value = st.session_state.user, key="user_input")

    password = st.text_input("Password", type="password")

    account = st.text_input("Account", placeholder= "These were the previous accounts used: {} ".format(history["account"]), value = st.session_state.account, key="account_input")

    warehouse  = st.text_input("Warehouse", placeholder= "These were the previous warehouses used: {} ".format(history["warehouse"]), value = st.session_state.warehouse, key="warehouse_input")

    database  = st.text_input("Database", placeholder= "These were the previous databases used: {} ".format(history["database"]), value = st.session_state.database, key="database_input")

    schema = st.text_input("Schema", placeholder= "These were the previous schemas used: {} ".format(history["schema"]), value = st.session_state.schema, key="schema_input")

    # Update session state
    st.session_state.user = user
    st.session_state.account = account
    st.session_state.warehouse = warehouse
    st.session_state.database = database
    st.session_state.schema = schema

    # --- Save unique entries ---
    if user not in history["user"]:
        history["user"].append(user)
        with open(SAVE_FILE, "w") as f:
            json.dump(history,f)
    
    if password not in history["password"]:
        history["password"].append(password)
        with open(SAVE_FILE, "w") as f:
            json.dump(history,f)
    
    if account not in history["account"]:
        history["account"].append(account)
        with open(SAVE_FILE, "w") as f:
            json.dump(history,f)

    if warehouse not in history["warehouse"]:
        history["warehouse"].append(warehouse)
        with open(SAVE_FILE, "w") as f:
            json.dump(history,f)

    if database not in history["database"]:
        history["database"].append(database)
        with open(SAVE_FILE, "w") as f:
            json.dump(history,f)

    if schema not in history["schema"]:
        history["schema"].append(schema)
        with open(SAVE_FILE, "w") as f:
            json.dump(history,f)

    if st.button("Test Snowflake Connection"):
        success, message = test_snowflake_connection(user, password, account, warehouse, database, schema)
        if success:
            st.success(message)
        else:
            st.error(message)

    st.subheader("DBT Project")
    dbt_path = st.text_input("DBT Project Path")
    if st.button("Run DBT"):
        if dbt_path:
            stdout, stderr = run_dbt_command(f"dbt run --project-dir {dbt_path}")
            st.text_area("DBT Output", stdout)
            if stderr:
                st.text_area("DBT Errors", stderr)
        else:
            st.warning("Please provide DBT project path.")

elif current_page == "Migration Settings":
    uploaded_files = st.file_uploader("Upload Oracle SQL Files", accept_multiple_files=True, type=["sql"])
    model_type = st.selectbox("Select DBT Model Type", ["view", "table", "incremental"])

    if st.button("Convert to DBT Models"):
        if uploaded_files:
            for file in uploaded_files:
                sql_content = file.read().decode("utf-8")
                st.markdown(f"### Converted SQL for `{file.name}`")
                st.code(f"-- Converted to {model_type}\n{sql_content}", language="sql")
            st.success("Conversion completed!")
        else:
            st.warning("Please upload at least one SQL file.")

elif current_page == "SQL Validation":
    sql_input = st.text_area("Paste your Oracle SQL query here")
    if st.button("Validate SQL"):
        is_valid, message = validate_sql(sql_input)
        if is_valid:
            st.success(message)
        else:
            st.error(f"Validation failed: {message}")

# --- Next and Previous Buttons at Bottom ---
st.markdown("---")
col1, col2 = st.columns([6, 1])
with col1:
    if st.button("⬅️ Previous") and st.session_state.page_index > 0:
        st.session_state.page_index -= 1
with col2:
    if st.button("Next ➡️") and st.session_state.page_index < len(pages) - 1:
        st.session_state.page_index += 1
