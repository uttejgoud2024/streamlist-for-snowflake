# core_logic_v4.py

import os
import re
import logging
import sqlparse
from pathlib import Path
from snowflake.snowpark import Session
from crewai import BaseLLM, Agent, Task, Crew
from typing import Union, List, Dict, Any

# --- Configuration and Setup ---
def log_setup(log_dir, file_name):
    """Sets up file and stream logging, removing old handlers."""
    os.makedirs(log_dir, exist_ok=True)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(log_dir, file_name), mode='a'),
            logging.StreamHandler()
        ]
)

# --- SQL Validation and Cleaning ---
def validate_sql(sql_text):
    """Validates SQL syntax and checks for DML statements."""
    logging.info("Starting SQL validation...")
    try:
        if not sql_text.strip():
            logging.warning("Empty or invalid SQL text provided.")
            return False, "Empty or invalid SQL."
        
        parsed = sqlparse.parse(sql_text)
        if not parsed or len(parsed) == 0:
            logging.warning("SQL parsing resulted in no statements.")
            return False, "Empty or invalid SQL."

        for statement in parsed:
            statement_type = statement.get_type()
            # Added "CTE" to handle 'WITH' clauses
            if statement_type.upper() not in ('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH', 'CTE'):
                logging.error(f"Unsupported SQL statement type: {statement_type}.")
                return False, f"Unsupported SQL statement type: {statement_type}. Only DML is allowed."

        logging.info("SQL syntax looks valid.")
        return True, "SQL syntax looks valid."
    except Exception as e:
        logging.error(f"SQL validation failed due to an exception: {e}")
        return False, f"SQL syntax error: {str(e)}"

def convert_oracle_to_snowflake(sql_text):
    """Converts common Oracle functions and syntax to their Snowflake equivalents."""
    logging.info("Starting Oracle to Snowflake syntax conversion...")
    
    # Simple regex replacements
    sql_text = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bNVL\s*\(([^,]+),\s*([^)]+)\)', r'COALESCE(\1, \2)', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bTO_DATE\s*\(([^,]+),\s*([^)]+)\)', r"TO_DATE(\1, \2)", sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bTO_CHAR\s*\(([^,]+),\s*([^)]+)\)', r"TO_VARCHAR(\1, \2)", sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bTO_NUMBER\s*\(([^)]+)\)', r'TRY_TO_NUMBER(\1)', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\(\+\)', '', sql_text)
    sql_text = re.sub(r'\bROWNUM\s*<=\s*(\d+)', r'LIMIT \1', sql_text, flags=re.IGNORECASE)
    
    # Improved DECODE conversion logic
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

    sql_text = re.sub(r'\bDECODE\s*\((.*?)\)', decode_to_case, sql_text, flags=re.IGNORECASE | re.DOTALL)
    
    # Improved SUBSTR conversion logic
    sql_text = re.sub(r'\bSUBSTR\s*\(([^,]+),\s*([^,]+),\s*([^)]+)\)', r'SUBSTRING(\1, \2, \3)', sql_text, flags=re.IGNORECASE)
    sql_text = re.sub(r'\bSUBSTR\s*\(([^,]+),\s*([^)]+)\)', r'SUBSTRING(\1, \2)', sql_text, flags=re.IGNORECASE)

    logging.info("Conversion completed.")
    return sql_text

def wrap_sql_in_dbt_model(sql_text, model_type):
    """Wraps the SQL in a DBT config block."""
    logging.info("Wrapping SQL in DBT model config...")
    config = f"{{{{ config(materialized='{model_type}') }}}}"
    return f"{config}\n\n{sql_text}"

def create_summary_file(output_dir, file_name, model_type, status_message):
    """Creates a summary file with migration details, appending to the file."""
    summary_path = Path(output_dir) / "summary.txt"
    with open(summary_path, "a") as f:
        f.write("--- Migration Summary for " + file_name + " ---\n\n")
        f.write(f"File Name: {file_name}\n")
        f.write(f"DBT Model Type: {model_type}\n")
        f.write(f"Migration Status: {status_message}\n")
        f.write("\n" + "-" * 30 + "\n\n")
    logging.info(f"Summary for {file_name} appended to {summary_path}")
    return summary_path

# --- Snowflake Cortex LLM and CrewAI ---
class SnowflakeCortexLLM(BaseLLM):
    """Custom LLM class to integrate with Snowflake Cortex AI."""
    def __init__(self, sp_session: Session, model: str = "llama3-8b"):
        super().__init__(model=model)
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
            logging.debug(f"Calling Cortex with prompt: {safe_prompt[:100]}...")

            result_df = self.sp_session.sql(
                f"SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(model => '{self.model}', prompt => '{safe_prompt}')"
            ).collect()
            
            response = result_df[0][0] if result_df and result_df[0][0] else "No response from Cortex."
            logging.debug(f"Cortex response received: {response[:100]}...")
            return response
        except Exception as e:
            logging.error(f"❌ Error calling Cortex model: {e}")
            return f"Error during model execution: {e}"

    def supports_function_calling(self) -> bool:
        return False

    def get_context_window_size(self) -> int:
        return 8192

def get_snowpark_session_and_llm():
    """Initializes and caches the Snowflake session and custom LLM."""
    logging.info("Attempting to create Snowflake session and Cortex LLM...")
    try:
        connection_parameters = {
            "user": os.getenv('SNOWFLAKE_USER'),
            "password": os.getenv('SNOWFLAKE_PASSWORD'),
            "account": os.getenv('SNOWFLAKE_ACCOUNT'),
            "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
            "database": os.getenv('SNOWFLAKE_DATABASE'),
            "schema": os.getenv('SNOWFLAKE_SCHEMA'),
            "role": os.getenv('SNOWFLAKE_ROLE')
        }
        
        missing_vars = [key for key, value in connection_parameters.items() if not value]
        if missing_vars:
            logging.critical(f"❌ Failed to create Snowflake session: Missing environment variables: {', '.join(missing_vars)}")
            return None, None
            
        session = Session.builder.configs(connection_parameters).create()
        llm = SnowflakeCortexLLM(sp_session=session)
        logging.info("✅ Snowflake session and LLM created successfully.")
        return session, llm
    except Exception as e:
        logging.critical(f"❌ Failed to create Snowflake session: {e}")
        return None, None

def run_crew_migration(file_content, source_type, model_type, custom_llm):
    """Runs the CrewAI process for procedural code migration."""
    try:
        oracle_analyst = Agent(role="Oracle PL/SQL Analyst", goal="Analyze and explain the logic of Oracle procedures, functions, packages, and views.", backstory="A seasoned expert in Oracle PL/SQL, meticulously breaking down complex business logic, procedural constructs (BEGIN/END blocks, FOR loops, IF/ELSE statements), and database interactions.", llm=custom_llm, verbose=True)
        dbt_modeler = Agent(role="Snowflake DBT Modeler", goal="Translate Oracle procedural and declarative logic into clean, efficient, and modular Snowflake dbt models.", backstory="A master of Snowflake SQL and DBT best practices. This agent focuses on converting imperative procedural logic into a single, declarative SQL query that can be run as a dbt model.", llm=custom_llm, verbose=True)
        snowflake_optimizer = Agent(role="Snowflake Optimizer", goal="Refactor and optimize the converted SQL for Snowflake's architecture, ensuring maximum performance.", backstory="A performance engineer with deep knowledge of Snowflake's query engine, ensuring all code runs at peak efficiency. This agent applies best practices like QUALIFY, ROW_NUMBER, and proper join techniques.", llm=custom_llm, verbose=True)
        quality_reviewer = Agent(role="SQL Quality Reviewer", goal="Validate the final DBT model for correctness, formatting, and adherence to standards.", backstory="A meticulous reviewer who ensures the final output is production-ready, well-formatted, and follows coding standards.", llm=custom_llm, verbose=True)

        tasks = [
            Task(description=f"Analyze the following Oracle {source_type} code and document its core business logic:\n\n{file_content}", expected_output=f"A clear, structured document explaining the {source_type.lower()}'s logic.", agent=oracle_analyst),
            Task(description="Based on the analysis, convert the procedural logic into a single, executable SQL SELECT statement for a Snowflake DBT model. Do NOT include DDL statements.", expected_output="A single, well-formatted DBT model SQL file (a SELECT statement).", agent=dbt_modeler),
            Task(description="Review and optimize the converted SQL for Snowflake's architecture, focusing on performance.", expected_output="An optimized DBT model SQL file.", agent=snowflake_optimizer),
            Task(description="Review the final, optimized DBT model SQL for correctness, formatting, and style. The output should be the final production-ready SQL.", expected_output="The final, production-ready DBT model SQL, formatted with correct indentation and comments.", agent=quality_reviewer)
        ]
        
        crew = Crew(
            agents=[oracle_analyst, dbt_modeler, snowflake_optimizer, quality_reviewer],
            tasks=tasks,
            verbose=True
        )
        
        final_output = crew.kickoff()
        logging.info("CrewAI execution completed.")
        
        final_output_str = final_output if isinstance(final_output, str) else str(final_output)

        # Extract SQL from the markdown block
        clean_sql = ""
        sql_match = re.search(r"```sql\s*(.*?)\s*```", final_output_str, re.DOTALL)
        if sql_match:
            clean_sql = sql_match.group(1).strip()
        else:
            # Fallback to the last part of the output if no markdown block is found
            clean_sql = final_output_str.split("Final Answer:")[-1].strip()
        
        if not clean_sql:
            raise ValueError("No SQL code was generated by the CrewAI agents.")
        
        converted_sql = convert_oracle_to_snowflake(clean_sql)
        wrapped_sql = wrap_sql_in_dbt_model(converted_sql, model_type)
        
        return wrapped_sql, "Success"

    except Exception as e:
        logging.critical(f"CrewAI execution failed with an exception: {e}")
        return None, f"Failure: {e}"
