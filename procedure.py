
# Required Libraries
from snowflake.snowpark import Session
from crewai import BaseLLM, Agent, Task, Crew
from typing import Union, List, Dict, Any
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

# Snowflake connection parameters
connection_parameters = {
    "account": "BBYUPDD-OF13152",
    "user": "Sathyasree",
    "password": "Sathyasree@2899",  # 🔐 Use environment variables in production
    "role": "ACCOUNTADMIN",
    "warehouse": "cortex_wh",
    "database": "cortex_demo",
    "schema": "analysis"
}

# Create Snowflake session
try:
    session = Session.builder.configs(connection_parameters).create()
    logging.info("✅ Snowflake session created successfully.")
except Exception as e:
    logging.error(f"❌ Failed to create Snowflake session: {e}")
    raise

# Oracle procedure to convert
oracle_procedure = """
CREATE OR REPLACE PROCEDURE calc_customer_ltv AS
BEGIN
  INSERT INTO customer_ltv
  SELECT customer_id, SUM(order_value) * 0.1
  FROM orders WHERE order_date > ADD_MONTHS(SYSDATE, -12);
  COMMIT;
END;
"""

# Step 1: Define custom LLM using Snowflake Cortex
class SnowflakeCortexLLM(BaseLLM):
    def __init__(self, sp_session, model="llama3.1-8b", temperature=0.7):
        super().__init__(model=model, temperature=temperature)
        self.sp_session = sp_session

    def call(
        self,
        messages: Union[str, List[Dict[str, str]]],
        tools: List[dict] = None,
        callbacks: List[Any] = None,
        available_functions: Dict[str, Any] = None,
        **kwargs
    ) -> str:
        try:
            if isinstance(messages, list):
                prompt = "\n".join([msg["content"] for msg in messages if msg["role"] == "user"])
            else:
                prompt = messages

            if prompt is None:
                logging.error("❌ Prompt is None.")
                return "Error: No prompt provided."

            safe_prompt = prompt.replace("'", "''")

            result_df = self.sp_session.sql(
                f"SELECT SNOWFLAKE.CORTEX.AI_COMPLETE(model => '{self.model}', prompt => '{safe_prompt}')"
            ).collect()

            return result_df[0][0] if result_df else "No response from Cortex."

        except Exception as e:
            logging.error(f"❌ Error calling Cortex model: {e}")
            return "Error during model execution."

    def supports_function_calling(self) -> bool:
        return False

    def get_context_window_size(self) -> int:
        return 8192

# Step 2: Instantiate the custom LLM
custom_llm = SnowflakeCortexLLM(sp_session=session)

# Step 3: Define Agents
oracle_analyst = Agent(
    role="Oracle Analyst",
    goal="Understand and interpret legacy Oracle procedures.",
    backstory="A seasoned Oracle developer who specializes in legacy system analysis.",
    llm=custom_llm,
    verbose=True
)

dbt_modeler = Agent(
    role="DBT Modeler",
    goal="Translate SQL logic into DBT-compatible models.",
    backstory="An expert in DBT best practices and Snowflake optimization.",
    llm=custom_llm,
    verbose=True
)

snowflake_optimizer = Agent(
    role="Snowflake Optimizer",
    goal="Ensure the DBT model is optimized for Snowflake.",
    backstory="A performance engineer with deep knowledge of Snowflake internals.",
    llm=custom_llm,
    verbose=True
)

quality_reviewer = Agent(
    role="Quality Reviewer",
    goal="Validate the final DBT model for correctness and style.",
    backstory="A meticulous reviewer who ensures production-readiness.",
    llm=custom_llm,
    verbose=True
)

# Step 4: Define Tasks with distinct prompts
task1 = Task(
    description=f"Analyze the following Oracle procedure and explain its logic and business purpose:\n\n{oracle_procedure}",
    expected_output="A clear explanation of the procedure's logic and business intent.",
    agent=oracle_analyst
)

task2 = Task(
    description="Using the explanation from the Oracle Analyst, convert the logic into a DBT model SQL file for Snowflake. Follow DBT conventions and ensure proper formatting.",
    expected_output="A DBT model SQL file that replicates the Oracle procedure logic.",
    agent=dbt_modeler
)

task3 = Task(
    description="Optimize the DBT model SQL for Snowflake performance. Apply best practices such as efficient joins, partitioning, and use of Snowflake functions.",
    expected_output="An optimized DBT model SQL file with Snowflake-specific enhancements.",
    agent=snowflake_optimizer
)

task4 = Task(
    description="Review the final DBT model SQL for correctness, formatting, and style. Ensure it is production-ready and follows DBT standards.",
    expected_output="A validated and production-ready DBT model SQL file.",
    agent=quality_reviewer
)

# Step 5: Create Crew and run
crew = Crew(
    agents=[oracle_analyst, dbt_modeler, snowflake_optimizer, quality_reviewer],
    tasks=[task1, task2, task3, task4]
)

try:
    result = crew.kickoff()
    print("\n🧠 Final DBT Model SQL:\n")

    # Format the output properly
    dbt_sql = str(result)
    formatted_sql = dbt_sql.replace("\\n", "\n").replace("\\t", "\t")

    print(formatted_sql)

    # Save the result to a .sql file
    try:
        with open("customer_orders_model.sql", "w") as file:
            file.write(formatted_sql)
        print("✅ DBT model SQL code saved to 'customer_orders_model.sql'.")
    except Exception as e:
        print(f"❌ Error saving DBT model SQL: {e}")

except Exception as e:
    logging.error(f"❌ Error running CrewAI: {e}")

# Close session
session.close()