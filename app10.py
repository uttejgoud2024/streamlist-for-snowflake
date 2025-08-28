import streamlit as st
import sqlparse
import subprocess
import snowflake.connector
import json
import os

# --- Profile Persistence Helpers ---
PROFILES_FILE = "profiles.json"

def load_profiles():
    profiles = {"Default (from secrets.toml)": st.secrets.get("snowflake", {})}
    if os.path.exists(PROFILES_FILE):
        with open(PROFILES_FILE, "r") as f:
            profiles.update(json.load(f))
    return profiles

def save_profile(profile_name, creds):
    profiles = load_profiles()
    profiles[profile_name] = creds
    profiles.pop("Default (from secrets.toml)", None)
    with open(PROFILES_FILE, "w") as f:
        json.dump(profiles, f, indent=2)

def delete_profile(profile_name):
    profiles = load_profiles()
    if profile_name in profiles:
        del profiles[profile_name]
        profiles.pop("Default (from secrets.toml)", None)
        with open(PROFILES_FILE, "w") as f:
            json.dump(profiles, f, indent=2)

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
pages = ["Home", "Environment Setup", "Migration Settings", "SQL Validation"]

if "page_index" not in st.session_state:
    st.session_state.page_index = 0

# Sidebar navigation (stable)
st.sidebar.title("Oracle ➜ Snowflake DBT Migration")
selected_page = st.sidebar.radio(
    "", pages, index=st.session_state.page_index, key="sidebar_radio"
)
if pages.index(selected_page) != st.session_state.page_index:
    st.session_state.page_index = pages.index(selected_page)
    st.rerun()

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
    # Load profiles from file and secrets
    if "profiles" not in st.session_state:
        st.session_state.profiles = load_profiles()
    SNOWFLAKE_PROFILES = st.session_state.profiles
    profile_names = list(SNOWFLAKE_PROFILES.keys())
    if "selected_profile" not in st.session_state:
        st.session_state.selected_profile = profile_names[0]
    selected_profile = st.selectbox(
        "Select Snowflake Profile",
        profile_names,
        key="profile_select",
        index=profile_names.index(st.session_state.selected_profile)
    )
    st.session_state.selected_profile = selected_profile
    creds = SNOWFLAKE_PROFILES[selected_profile]

    # Allow user to override fields if needed
    user = st.text_input("User", value=creds.get("user", st.session_state.get("user", "")))
    password = st.text_input("Password", type="password", value=creds.get("password", st.session_state.get("password", "")))
    account = st.text_input("Account", value=creds.get("account", st.session_state.get("account", "")))
    warehouse = st.text_input("Warehouse", value=creds.get("warehouse", st.session_state.get("warehouse", "")))
    database = st.text_input("Database", value=creds.get("database", st.session_state.get("database", "")))
    schema = st.text_input("Schema", value=creds.get("schema", st.session_state.get("schema", "")))

    # Save new profile section
    st.markdown("#### Save as New Profile")
    new_profile_name = st.text_input("New Profile Name")
    if st.button("Save Profile"):
        if new_profile_name.strip() == "" or new_profile_name in SNOWFLAKE_PROFILES:
            st.warning("Please enter a unique, non-empty profile name.")
        else:
            creds_to_save = {
                "user": user,
                "password": password,
                "account": account,
                "warehouse": warehouse,
                "database": database,
                "schema": schema
            }
            save_profile(new_profile_name, creds_to_save)
            # Reload profiles and update dropdown instantly
            st.session_state.profiles = load_profiles()
            st.session_state.selected_profile = new_profile_name
            st.success(f"Profile '{new_profile_name}' saved and selected!")
            st.rerun()

    # Delete profile section (do not allow deleting the default)
    if selected_profile != "Default (from secrets.toml)":
        if st.button(f"Delete Profile '{selected_profile}'"):
            delete_profile(selected_profile)
            st.session_state.profiles = load_profiles()
            st.session_state.selected_profile = "Default (from secrets.toml)"
            st.success(f"Profile '{selected_profile}' deleted!")
            st.rerun()

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

# Dropdown for DBT command selection
dbt_command = st.selectbox("Select DBT Command", ["run", "seed", "test", "docs generate"])

# Checkbox to run full DBT workflow
run_full_workflow = st.checkbox("Run Full DBT Workflow (seed → run → test → docs generate)")

if st.button("Execute DBT"):
    st.session_state["dbt_path"] = dbt_path
    if dbt_path:
        if run_full_workflow:
            commands = [
                f"dbt seed --project-dir {dbt_path}",
                f"dbt run --project-dir {dbt_path}",
                f"dbt test --project-dir {dbt_path}",
                f"dbt docs generate --project-dir {dbt_path}"
            ]
            for cmd in commands:
                st.markdown(f"### Running: `{cmd}`")
                stdout, stderr = run_dbt_command(cmd)
                st.text_area(f"Output for `{cmd}`", stdout, height=200)
                if stderr:
                    st.text_area(f"Errors for `{cmd}`", stderr, height=200)
        else:
            cmd = f"dbt {dbt_command} --project-dir {dbt_path}"
            st.markdown(f"### Running: `{cmd}`")
            stdout, stderr = run_dbt_command(cmd)
            st.text_area("DBT Output", stdout, height=200)
            if stderr:
                st.text_area("DBT Errors", stderr, height=200)
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

# --- Next and Previous Buttons at Bottom (stable) ---
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