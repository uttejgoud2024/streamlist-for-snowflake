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
                            
                            crew = Crew(agents=[oracle_analyst, dbt_modeler, snowflake_optimizer, quality_reviewer], tasks=[task1, task2, task3, task4], verbose=True)
                            
                            try:
                                llm_result = crew.kickoff()
                                
                                # --- START OF NEW FIX ---
                                # Use regex to extract only the SQL code block from the AI's response
                                sql_match = re.search(r'```sql\n(.*?)```', llm_result, re.DOTALL)
                                if sql_match:
                                    clean_sql = sql_match.group(1).strip()
                                else:
                                    # If no markdown block is found, assume the entire result is the SQL
                                    clean_sql = str(llm_result).strip()
                                    
                                # Final check for remaining Oracle syntax
                                converted_sql = convert_oracle_to_snowflake(clean_sql)
                                wrapped_sql = wrap_sql_in_dbt_model(converted_sql, model_type)
                                # --- END OF NEW FIX ---
                                
                                status.update(label="Migration complete!", state="complete", expanded=False)
                            except Exception as e:
                                status.update(label="Migration failed.", state="error", expanded=False)
                                st.error(f"❌ CrewAI execution failed: {e}")
                                continue
