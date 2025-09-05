# streamlit_app.py

import streamlit as st
import os
import re
import logging
import dotenv # Import the dotenv library
from pathlib import Path
from core_logic import (
    get_snowpark_session_and_llm,
    validate_sql,
    convert_oracle_to_snowflake,
    wrap_sql_in_dbt_model,
    create_summary_file,
    run_crew_migration
)


# Load environment variables from a .env file
dotenv.load_dotenv()

# --- CSS for UI
st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1rem;
        padding-bottom: 0rem;
        padding-left: 5rem;
        padding-right: 5rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- Streamlit UI Functions ---
def home_tab():
    st.markdown("### Introduction")
    st.markdown("**A Python-powered Streamlit app that helps migrate Oracle SQL queries and procedures to Snowflake DBT models.**")
    st.markdown("#### Features")
    st.markdown("- ✅ **SQL Validation**: Ensures your Oracle SQL syntax is correct.")
    st.markdown("- 🔄 **Automated Conversion**: Automatically translates common Oracle functions to Snowflake syntax.")
    st.markdown("- 🤖 **AI-Powered Migration**: Uses Snowflake Cortex and CrewAI for complex procedure logic conversion.")
    st.markdown("- 📦 **Bulk Migration**: Upload and process multiple SQL files in one go.")
    st.markdown("- 🧱 **DBT Model Wrapping**: Wraps converted SQL into DBT-compatible models.")

def setup_tab():
    st.markdown("## ⚙️ Environment Setup")
    st.markdown("### 🛠️ Pre-Requisites")
    st.markdown("1. **Snowflake Account:** Ensure you have a Snowflake account with **Cortex AI** enabled.")
    st.markdown("2. **Python Packages:** Install the required packages.")
    st.code("pip install snowflake-snowpark-python crewai streamlit sqlparse python-dotenv")
    st.markdown("3. **DBT Project:** Set up a dbt project and configure your Snowflake connection in `profiles.yml`.")

    with st.expander("View an example `profiles.yml`"):
        st.code("""
your_project_name:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_snowflake_account>
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      schema: "{{ env_var('SNOWFLAKE_SCHEMA') }}"
      threads: 4
        """, language="yaml")
    
    dbt_path = st.text_input("DBT Project Path", value=st.session_state.get("dbt_path", str(Path.cwd())))
    subfolder = st.text_input("Subfolder inside models (optional)", value="oracle_migration")
    
    st.session_state["dbt_path"] = dbt_path
    st.session_state["subfolder"] = subfolder

def migration_settings_tab(session, custom_llm):
    st.markdown("## 📁 Migration Settings")
    
    col1, col2 = st.columns(2)
    with col1:
        source_type = st.selectbox("Select Source File Type", ["SQL File", "Procedure", "Function", "Package", "View"])
    with col2:
        model_type = st.selectbox("Select DBT Model Type", ["view", "table"])
    
    st.markdown("### 📂 Upload Oracle Files")
    uploaded_files = st.file_uploader(
        "Drag and drop Oracle files here",
        type=["sql", "txt"],
        accept_multiple_files=True
    )

    dbt_path = st.session_state.get("dbt_path")
    subfolder = st.session_state.get("subfolder")
    
    output_dir, log_dir = None, None
    if dbt_path:
        output_dir = Path(dbt_path) / "models" / subfolder
        log_dir = Path(dbt_path) / "migration_logs"
        output_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)
    else:
        st.warning("⚠️ Please provide a valid DBT project path to save models and logs.")
        return
    
    if st.button("🚀 Convert and Save Models"):
        if uploaded_files:
            total_files = len(uploaded_files)
            for i, file in enumerate(uploaded_files):
                st.subheader(f"Processing file {i+1} of {total_files}: `{file.name}`")
                
                try:
                    file_content = file.read().decode("utf-8")
                    base_name = os.path.splitext(file.name)[0]
                    safe_name = re.sub(r'[^a-zA-Z0-9_.]', '_', base_name)
                    output_filename = output_dir / f"{safe_name}.sql"
                    
                    if source_type == "SQL File":
                        is_valid, message = validate_sql(file_content)
                        if not is_valid:
                            st.error(f"Validation failed for `{file.name}`: {message}")
                            continue
                        
                        with st.spinner(f"Converting `{file.name}` using regex..."):
                            converted_sql = convert_oracle_to_snowflake(file_content)
                            wrapped_sql = wrap_sql_in_dbt_model(converted_sql, model_type)
                            oracle_logic_summary = "N/A - Direct SQL Conversion"
                            create_summary_file(log_dir, file.name, model_type, oracle_logic_summary)
                            
                    elif source_type in ["Procedure", "Function", "Package", "View"]:
                        if not custom_llm:
                            st.error("❌ Snowflake Cortex LLM is not initialized. Cannot process this file type.")
                            continue

                        with st.status(f"Using CrewAI to convert `{file.name}`...", expanded=True) as status:
                            try:
                                wrapped_sql, oracle_logic_summary = run_crew_migration(file_content, source_type, model_type, custom_llm)
                                create_summary_file(log_dir, file.name, model_type, oracle_logic_summary)
                                status.update(label="✅ **Migration complete!**", state="complete", expanded=False)
                            except Exception as e:
                                logging.critical(f"CrewAI execution failed with an exception: {e}")
                                status.update(label="❌ **Migration failed.**", state="error", expanded=False)
                                st.error(f"❌ CrewAI execution failed: {e}")
                                continue
                    
                    with open(output_filename, "w") as f:
                        f.write(wrapped_sql)
                    
                    st.success(f"✅ Converted and saved to `{output_filename}`")
                    
                    with st.expander("View Original and Converted Code"):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.markdown(f"#### 📥 Original Oracle Code for `{file.name}`")
                            st.code(file_content, language="sql")
                    with col2:
                        st.markdown(f"#### 📤 Converted Snowflake DBT Model")
                        cleaned_sql = wrapped_sql.strip().replace("\\n", "\n").replace("&gt;", ">")
                        st.code(cleaned_sql, language="sql")
                    with open(output_filename, "rb") as f:
                        st.download_button(label=f"⬇️ Download `{safe_name}.sql`", data=f, file_name=f"{safe_name}.sql", mime="text/sql")
                except Exception as e:
                    st.error(f"❌ Error processing `{file.name}`: {str(e)}")
        else:
            st.warning("⚠️ Please upload at least one file and provide a valid DBT path.")

    if log_dir and Path(log_dir).exists() and (Path(log_dir) / "summary.txt").exists():
        st.markdown("### Migration Summary Report")
        with st.expander("View Full Summary"):
            summary_path = Path(log_dir) / "summary.txt"
            with open(summary_path, "r") as f:
                st.text(f.read())

def main():
    st.set_page_config(page_title="Oracle to Snowflake DBT Migration", layout="wide")
    st.markdown("<h1 style='text-align: center; color: #2E86C1;'>🚀 Oracle to Snowflake DBT Migration</h1>", unsafe_allow_html=True)
    tab1, tab2, tab3 = st.tabs(["🏠 Home", "⚙️ Environment Setup", "📁 Migration Settings"])

    session, custom_llm = get_snowpark_session_and_llm()

    with tab1:
        home_tab()
    with tab2:
        setup_tab()
    with tab3:
        migration_settings_tab(session, custom_llm)

if __name__ == "__main__":
    main()

