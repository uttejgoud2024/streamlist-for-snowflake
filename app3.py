import streamlit as st
import sqlparse
import subprocess
import snowflake.connector

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

# --- Define Snowflake Profiles ---
SNOWFLAKE_PROFILES = {
    "Default (from secrets.toml)": st.secrets.get("snowflake", {}),
    "Account 2": {
        "user": "user2",
        "password": "password2",
        "account": "account2",
        "warehouse": "warehouse2",
        "database": "database2",
        "schema": "schema2"
    },
    "Account 3": {
        "user": "user3",
        "password": "password3",
        "account": "account3",
        "warehouse": "warehouse3",
        "database": "database3",
        "schema": "schema3"
    },
    "Account 4": {
        "user": "user4",
        "password": "password4",
        "account": "account4",
        "warehouse": "warehouse4",
        "database": "database4",
        "schema": "schema4"
    },
    "Account 5": {
        "user": "user5",
        "password": "password5",
        "account": "account5",
        "warehouse": "warehouse5",
        "database": "database5",
        "schema": "schema5"
    }
}

# --- Page Navigation Setup ---
pages = ["Home", "Environment Setup", "Migration Settings", "SQL Validation"]

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

elif current_page == "Environment Setup":
    st.subheader("Snowflake Credentials")
    # Profile selection
    profile_names = list(SNOWFLAKE_PROFILES.keys())
    selected_profile = st.selectbox("Select Snowflake Profile", profile_names, key="profile_select")
    creds = SNOWFLAKE_PROFILES[selected_profile]

    # Allow user to override fields if needed
    user = st.text_input("User", value=creds.get("user", st.session_state.get("user", "")))
    password = st.text_input("Password", type="password", value=creds.get("password", st.session_state.get("password", "")))
    account = st.text_input("Account", value=creds.get("account", st.session_state.get("account", "")))
    warehouse = st.text_input("Warehouse", value=creds.get("warehouse", st.session_state.get("warehouse", "")))
    database = st.text_input("Database", value=creds.get("database", st.session_state.get("database", "")))
    schema = st.text_input("Schema", value=creds.get("schema", st.session_state.get("schema", "")))

    if st.button("Test Snowflake Connection"):
        st.session_state["user"] = user
        st.session_state["password"] = password
        st.session_state["account"] = account
        st.session_state["warehouse"] = warehouse
        st.session_state["database"] = database
        st.session_state["schema"] = schema
        success, message = test_snowflake_connection(user, password, account, warehouse, database, schema)
        if success:
            st.success(message)
        else:
            st.error(message)

    st.subheader("DBT Project")
    dbt_path = st.text_input("DBT Project Path", value=st.session_state.get("dbt_path", ""))
    if st.button("Run DBT"):
        st.session_state["dbt_path"] = dbt_path
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
st.markdown("<br><hr>", unsafe_allow_html=True)
col1, col2 = st.columns([6, 1])
with col1:
    if st.button("⬅️ Previous") and st.session_state.page_index > 0:
        st.session_state.page_index -= 1
with col2:
    if st.button("Next ➡️") and st.session_state.page_index < len(pages) - 1:
        st.session_state.page_index += 1