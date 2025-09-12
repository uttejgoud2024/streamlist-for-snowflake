# streamlit_app_v5.py

from sysconfig import get_path
import streamlit as st
import os
import re
import logging
import dotenv 
from pathlib import Path
from core_logic_v5 import (
    get_snowpark_session_and_llm,
    validate_sql,
    convert_oracle_to_snowflake,
    wrap_sql_in_dbt_model,
    create_summary_file,
    run_crew_migration,
    log_setup
)
import zipfile
import io

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

def handle_conversion_process(uploaded_files, dbt_path, subfolder, source_type, model_type, session, custom_llm):
    """Encapsulates the file processing logic."""
    output_dir = Path(dbt_path) / "models" / subfolder
    log_dir = Path(dbt_path) / "migration_logs"
    
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        st.error(f"❌ Error creating directories: {e}. Please check permissions.")
        return

    # Call log_setup without a file_name to create a new, unique log file
    log_setup(log_dir=log_dir)
    
    # Clear previous summary file for a fresh start
    summary_path = log_dir / "summary.txt"
    if summary_path.exists():
        summary_path.unlink()
        
    st.session_state['uploaded_files_data'] = {file.name: file.read().decode("utf-8") for file in uploaded_files}
    st.session_state['converted_files'] = {}
    st.session_state['migration_summary_exists'] = False

    total_files = len(st.session_state['uploaded_files_data'])
    progress_bar = st.progress(0)
    status_placeholder = st.empty()

    for i, (file_name, file_content) in enumerate(st.session_state['uploaded_files_data'].items()):
        status_placeholder.info(f"Processing file {i+1} of {total_files}: `{file_name}`")
        
        try:
            base_name = os.path.splitext(file_name)[0]
            safe_name = re.sub(r'[^a-zA-Z0-9_.]', '_', base_name)
            output_filename = output_dir / f"{safe_name}.sql"
            
            if source_type == "SQL File":
                is_valid, message = validate_sql(file_content)
                if not is_valid:
                    st.error(f"Validation failed for `{file_name}`: {message}")
                    create_summary_file(log_dir, file_name, model_type, f"Failure: {message}")
                    continue
                
                with st.spinner(f"Converting `{file_name}` using regex..."):
                    converted_sql = convert_oracle_to_snowflake(file_content)
                    wrapped_sql = wrap_sql_in_dbt_model(converted_sql, model_type)
                    create_summary_file(log_dir, file_name, model_type, "Success: Converted via regex")
                    
            elif source_type in ["Procedure", "Function", "Package", "View"]:
                with st.status(f"Using CrewAI to convert `{file_name}`...", expanded=True) as status:
                    wrapped_sql, status_message = run_crew_migration(file_content, source_type, model_type, custom_llm)
                    
                    create_summary_file(log_dir, file_name, model_type, status_message)
                    
                    if "Success" in status_message:
                        status.update(label="✅ **Migration complete!**", state="complete", expanded=False)
                    else:
                        status.update(label="❌ **Migration failed.**", state="error", expanded=False)
                        st.error(f"❌ CrewAI execution failed: {status_message}")
                        continue
            
            if wrapped_sql:
                with open(output_filename, "w") as f:
                    f.write(wrapped_sql)
                st.session_state['converted_files'][file_name] = wrapped_sql
                st.success(f"✅ Converted and saved to `{output_filename}`")
            else:
                st.warning(f"Conversion for `{file_name}` produced no output.")
            
        except Exception as e:
            st.error(f"❌ Error processing `{file_name}`: {str(e)}")
            create_summary_file(log_dir, file_name, model_type, f"Failure: {e}")
            
        st.session_state['migration_summary_exists'] = True
        progress_bar.progress((i + 1) / total_files)

    status_placeholder.empty() # Clear the status message
    st.toast("✅ All files processed!", icon="🎉")

def display_results():
    """Displays the summary and converted files section."""
    dbt_path = st.session_state.get("dbt_path")
    subfolder = st.session_state.get("subfolder")
    
    if not dbt_path or not subfolder:
        return

    log_dir = Path(dbt_path) / "migration_logs"
    output_dir = Path(dbt_path) / "models" / subfolder

    if log_dir.exists() and (log_dir / "summary.txt").exists():
        st.markdown("### Migration Summary Report")
        with st.expander("View Full Summary"):
            summary_path = log_dir / "summary.txt"
            with open(summary_path, "r") as f:
                st.text(f.read())
    
    if 'uploaded_files_data' in st.session_state and st.session_state['uploaded_files_data']:
        st.markdown("### Converted Files")
        
        # Create a zip file in memory for bulk download
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for file_name in st.session_state['converted_files'].keys():
                base_name = os.path.splitext(file_name)[0]
                safe_name = re.sub(r'[^a-zA-Z0-9_.]', '_', base_name)
                output_filename = output_dir / f"{safe_name}.sql"
                if output_filename.exists():
                    zip_file.write(output_filename, arcname=f"{safe_name}.sql")
        
        # Add the download button for the zip file
        if st.session_state['converted_files']:
            st.download_button(
                label="⬇️ Download All Converted Files (.zip)",
                data=zip_buffer.getvalue(),
                file_name="converted_dbt_models.zip",
                mime="application/zip",
            )
        
        # Display each file individually
        for file_name, original_content in st.session_state['uploaded_files_data'].items():
            if file_name in st.session_state['converted_files']:
                wrapped_sql = st.session_state['converted_files'][file_name]
                
                with st.expander(f"View Original and Converted Code for `{file_name}`"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"#### 📥 Original Oracle Code")
                        st.code(original_content, language="sql")
                    with col2:
                        st.markdown(f"#### 📤 Converted Snowflake DBT Model")
                        cleaned_sql = wrapped_sql.strip().replace("\\n", "\n").replace("&gt;", ">")
                        st.code(cleaned_sql, language="sql")
                    
                    base_name = os.path.splitext(file_name)[0]
                    safe_name = re.sub(r'[^a-zA-Z0-9_.]', '_', base_name)
                    output_filename = output_dir / f"{safe_name}.sql"
                    
                    if output_filename.exists():
                        with open(output_filename, "rb") as f:
                            st.download_button(label=f"⬇️ Download `{safe_name}.sql`", data=f, file_name=f"{safe_name}.sql", mime="text/sql")

def migration_settings_tab(session, custom_llm):
    st.markdown("## 📁 Migration Settings")
    
    if not session or not custom_llm:
        st.error("❌ Failed to connect to Snowflake. Please check your environment variables and try again.")
        return

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
    
    if st.button("🚀 Convert and Save Models"):
        if not uploaded_files:
            st.warning("⚠️ Please upload at least one file.")
        elif not dbt_path:
            st.warning("⚠️ Please provide a valid DBT path to save models and logs.")
        else:
            handle_conversion_process(uploaded_files, dbt_path, subfolder, source_type, model_type, session, custom_llm)

    display_results()
        
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

