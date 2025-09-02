import streamlit as st
import sqlparse
import subprocess
import re
import os
import shlex
import logging
from snowflake.snowpark import Session
from crewai import BaseLLM, Agent, Task, Crew
from typing import Union, List, Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- SQL Validation ---
def validate_sql(sql_text):
    """
    Validates SQL syntax and checks for DML statements.
    """
    try:
        if not sql_text.strip():
            return False, "Empty or invalid SQL."
        
        parsed = sqlparse.parse(sql_text)
        if not parsed or len(parsed) == 0:
            return False, "Empty or invalid SQL."

        for statement in parsed:
            statement_type = statement.get_type()
            if statement_type not in ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH'):
                return False, f"Unsupported SQL statement type: {statement_type}. Only DML is allowed."

        return True, "SQL syntax looks valid."
    except Exception as e:
        return False, f"SQL syntax error: {str(e)}"

# --- Strip DDL Statements ---
def strip_ddl(sql_text):
    """
    Strips CREATE, ALTER, and DROP statements from the SQL text.
    """
    sql_text = re.sub(r'(?i)(CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE)\s+(OR\s+REPLACE\s+)?(VIEW|TABLE|PROCEDURE|FUNCTION|INDEX|SEQUENCE|PACKAGE)\s+[^\n]+\n?', '', sql_text)
    sql_text = re.sub(r'(?i)^AS\s*\n?', '', sql_text)
    return sql_text.strip()

# --- Oracle to Snowflake SQL Conversion (Regex-based) ---
def convert_oracle_to_snowflake(sql_text):
    """
    Converts common Oracle functions and syntax to their Snowflake equivalents.
    """
    sql_text = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bNVL\s*\(([^,]+),\s*([^)]+)\)', r'COALESCE(\1, \2)', sql_text, flags=re.IGNORECASE)
    
    def decode_to_case(match):
        args = [arg.strip() for arg in match.group(1).split(',')]
        if len(args) < 3:
            return match.group(0)
        
        case_statement = f"CASE {args[0]} "
        for i in range(1, len(args) - 1, 2):
            case_statement += f"WHEN {args[i]} THEN {args[i+1]} "
        
        if len(args) % 2 != 0:
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

# --- Snowflake Cortex LLM and CrewAI ---
class SnowflakeCortexLLM(BaseLLM):
    def __init__(self, sp_session: Session, model: str = "llama3.1-8b", temperature: float = 0.7):
        super().__init__(model=model, temperature=temperature)
        self.sp_session = sp_session

    def call(self, messages: Union[str, List[Dict[str, str]]], **kwargs) -> str:
        try:
            if isinstance(messages, list):
                prompt = "\n".join([msg["content"] for msg in messages if msg["role"] == "user"])
            else:
                prompt = messages

            if not prompt:
                logging.error("❌ Prompt is empty or not provided.")
                return "Error: No prompt provided."

            safe_prompt = prompt.replace("'", "''")

            result_df = self.sp_session.sql(
                f"SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(model => '{self.model}', prompt => '{safe_prompt}')"
            ).collect()

            return result_df[0][0] if result_df and result_df[0][0] else "No response from Cortex."
        except Exception as e:
            logging.error(f"❌ Error calling Cortex model: {e}")
            return f"Error during model execution: {e}"

    def supports_function_calling(self) -> bool:
        return False

    def get_context_window_size(self) -> int:
        return 8192

# --- Main Streamlit UI ---
st.set_page_config(page_title="Oracle to Snowflake DBT Migration", layout="wide")
tab1, tab2, tab3 = st.tabs(["🏠 Home", "⚙️ Environment Setup", "📁 Migration Settings"])

# --- Snowflake Session and LLM Initialization ---
@st.cache_resource
def get_snowpark_session_and_llm():
    try:
        connection_parameters = {
            "account": st.secrets["snowflake"]["account"],
            "user": st.secrets["snowflake"]["user"],
            "password": st.secrets["snowflake"]["password"],
            "role": st.secrets["snowflake"]["role"],
            "warehouse": st.secrets["snowflake"]["warehouse"],
            "database": st.secrets["snowflake"]["database"],
            "schema": st.secrets["snowflake"]["schema"]
        }
        session = Session.builder.configs(connection_parameters).create()
        llm = SnowflakeCortexLLM(sp_session=session)
        logging.info("✅ Snowflake session and LLM created successfully.")
        return session, llm
    except Exception as e:
        st.error(f"❌ Failed to create Snowflake session or LLM. Please check your connection parameters. Error: {e}")
        return None, None

session, custom_llm = get_snowpark_session_and_llm()

with tab1:
    st.markdown("<h1 style='text-align: center; color: #2E86C1;'>🚀 Oracle to Snowflake DBT Migration</h1>", unsafe_allow_html=True)
    st.markdown("### Introduction")
    st.markdown("**A Python-powered Streamlit app that helps migrate Oracle SQL queries and procedures to Snowflake DBT models.**")
    st.markdown("#### Features")
    st.markdown("- ✅ SQL Validation: Ensures your Oracle SQL syntax is correct.")
    st.markdown("- 🔄 Automated Conversion: Automatically translates common Oracle functions to Snowflake syntax.")
    st.markdown("- 🤖 AI-Powered Migration: Uses Snowflake Cortex and CrewAI for complex procedure logic conversion.")
    st.markdown("- 📦 Bulk Migration: Upload and process multiple SQL files in one go.")
    st.markdown("- 🧱 DBT Model Wrapping: Wraps converted SQL into DBT-compatible models.")

with tab2:
    st.markdown("## ⚙️ Environment Setup")
    st.markdown("### 🛠️ Pre-Requisites")
    st.markdown("1. **Snowflake Account:** Ensure you have a Snowflake account with **Cortex AI** enabled.")
    st.markdown("2. **Python Packages:** Install the required packages.")
    st.code("pip install snowflake-snowpark-python crewai crewai_tools streamlit sqlparse pandas")
    st.markdown("3. **DBT Project:** Set up a dbt project and configure your Snowflake connection in `profiles.yml`.")

    dbt_path = st.text_input("DBT Project Path", value=st.session_state.get("dbt_path", ""))
    subfolder = st.text_input("Subfolder inside models (optional)", value="oracle_migration")
    
    st.session_state["dbt_path"] = dbt_path
    st.session_state["subfolder"] = subfolder

with tab3:
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
    
    output_dir = None
    if dbt_path:
        output_dir = os.path.join(dbt_path, "models", subfolder)
        os.makedirs(output_dir, exist_ok=True)
    else:
        st.warning("⚠️ Please provide a valid DBT project path to save models.")
    
    if st.button("🚀 Convert and Save Models"):
        if uploaded_files and output_dir:
            for file in uploaded_files:
                try:
                    file_content = file.read().decode("utf-8")
                    base_name = os.path.splitext(file.name)[0]
                    safe_name = re.sub(r'[^a-zA-Z0-9_.]', '_', base_name)
                    output_filename = os.path.join(output_dir, f"{safe_name}.sql")
                    
                    if source_type == "SQL File":
                        is_valid, message = validate_sql(file_content)
                        if not is_valid:
                            st.error(f"Validation failed for `{file.name}`: {message}")
                            continue
                        
                        with st.spinner(f"Converting `{file.name}` using regex..."):
                            converted_sql = convert_oracle_to_snowflake(file_content)
                            wrapped_sql = wrap_sql_in_dbt_model(converted_sql, model_type)
                            
                    elif source_type in ["Procedure", "Function", "Package", "View"]:
                        if not custom_llm:
                            st.error("❌ Snowflake Cortex LLM is not initialized. Cannot process this file type.")
                            continue

                        with st.status(f"Using CrewAI to convert `{file.name}`...", expanded=True) as status:
                            oracle_analyst = Agent(role="Oracle PL/SQL Analyst", goal="Analyze and explain the logic of Oracle procedures, functions, packages, and views.", backstory="A seasoned expert in Oracle PL/SQL, meticulously breaking down complex business logic, procedural constructs (BEGIN/END blocks, FOR loops, IF/ELSE statements), and database interactions.", llm=custom_llm, verbose=True)
                            dbt_modeler = Agent(role="Snowflake DBT Modeler", goal="Translate Oracle procedural and declarative logic into clean, efficient, and modular Snowflake dbt models.", backstory="A master of Snowflake SQL and DBT best practices. This agent focuses on converting imperative procedural logic into a single, declarative SQL query that can be run as a dbt model. It understands how to replace procedural constructs with efficient SQL statements.", llm=custom_llm, verbose=True)
                            snowflake_optimizer = Agent(role="Snowflake Optimizer", goal="Refactor and optimize the converted SQL for Snowflake's architecture, ensuring maximum performance.", backstory="A performance engineer with deep knowledge of Snowflake's query engine, ensuring all code runs at peak efficiency. This agent applies best practices like `QUALIFY`, `ROW_NUMBER`, and proper join techniques.", llm=custom_llm, verbose=True)
                            quality_reviewer = Agent(role="SQL Quality Reviewer", goal="Validate the final DBT model for correctness, formatting, and adherence to standards.", backstory="A meticulous reviewer who ensures the final output is production-ready, well-formatted, and follows coding standards.", llm=custom_llm, verbose=True)
                            
                            status.write("🕵️ Analyzing Oracle logic...")
                            task1 = Task(description=f"""
                                Analyze the following Oracle {source_type} code and document its core business logic.
                                The documentation must clearly explain:
                                1. The purpose and a high-level overview of the code.
                                2. Any variables, cursors, or loops used.
                                3. The main data flow, including source tables, filters, joins, and the final output or action.
                                4. How to convert procedural elements like BEGIN/END blocks, FOR loops, and IF/ELSE statements into a single, declarative SELECT statement.
                                Oracle {source_type} code:\n\n{file_content}
                            """, expected_output=f"A clear, structured document explaining the {source_type.lower()}'s logic and a plan for converting it to a declarative SQL query.", agent=oracle_analyst)

                            status.write("🤖 Translating to Snowflake SQL...")
                            task2 = Task(description=f"""
                                Based on the analysis from the Oracle PL/SQL Analyst, convert the procedural logic into a single DBT model SQL file for Snowflake.
                                The output must be a single, executable SQL SELECT statement that can be materialized as a {model_type}.
                                All procedural constructs (loops, conditional logic, etc.) must be replaced with equivalent declarative SQL (e.g., using CTEs, CASE statements, and set-based logic).
                                Do NOT include any DDL statements (CREATE, ALTER, DROP, etc.) or procedural blocks (BEGIN, END). The output should be pure SQL.
                            """, expected_output="A single, well-formatted DBT model SQL file (a SELECT statement) that can be run on Snowflake.", agent=dbt_modeler)

                            status.write("⚙️ Optimizing query for Snowflake...")
                            task3 = Task(description="""
                                Given the converted DBT model SQL, review and apply optimizations for Snowflake's architecture.
                                - Optimize joins and WHERE clauses.
                                - Use Snowflake-specific functions where they improve performance.
                                - Ensure the query is efficient for Snowflake's columnar storage and micro-partitioning.
                                The output must be the complete, optimized SQL query.
                            """, expected_output="An optimized DBT model SQL file with Snowflake-specific enhancements.", agent=snowflake_optimizer)
                            
                            status.write("✅ Final review and validation complete.")
                            task4 = Task(description="""
                                Review the final, optimized DBT model SQL.
                                Check for:
                                - Correctness: Does the SQL logic match the original business logic?
                                - Formatting: Is the code well-indented and easy to read?
                                - Style: Does it follow best practices for dbt and Snowflake?
                                - Final Output: The output should be the final, production-ready SQL.
                            """, expected_output="The final, production-ready DBT model SQL, formatted with correct indentation and comments.", agent=quality_reviewer)
                            
                            crew = Crew(agents=[oracle_analyst, dbt_modeler, snowflake_optimizer, quality_reviewer], tasks=[task1, task2, task3, task4], verbose=2)
                            
                            try:
                                llm_result = crew.kickoff()
                                wrapped_sql = wrap_sql_in_dbt_model(str(llm_result), model_type)
                                status.update(label="Migration complete!", state="complete", expanded=False)
                            except Exception as e:
                                status.update(label="Migration failed.", state="error", expanded=False)
                                st.error(f"❌ CrewAI execution failed: {e}")
                                continue
                    
                    with open(output_filename, "w") as f:
                        f.write(wrapped_sql)
                    
                    st.markdown("### Converted SQL")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"#### 📥 Original Oracle Code for `{file.name}`")
                        st.code(file_content, language="sql")
                    with col2:
                        st.markdown(f"#### 📤 Converted Snowflake DBT Model")
                        st.code(wrapped_sql, language="sql")
                    
                    with open(output_filename, "rb") as f:
                        st.download_button(label=f"⬇️ Download `{safe_name}.sql`", data=f, file_name=f"{safe_name}.sql", mime="text/sql")
                    
                    st.success(f"✅ Saved to `{output_filename}`")
                except Exception as e:
                    st.error(f"❌ Error processing `{file.name}`: {str(e)}")
        else:
            st.warning("⚠️ Please upload at least one file and provide a valid DBT path.")
