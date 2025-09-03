                            crew = Crew(agents=[oracle_analyst, dbt_modeler, snowflake_optimizer, quality_reviewer], tasks=[task1, task2, task3, task4], verbose=True)
                            
                            try:
                                llm_result = crew.kickoff()
                                logging.info("CrewAI execution completed.")
                                
                                final_output_str = llm_result.get('final_task_output', '') if isinstance(llm_result, dict) else str(llm_result)
                                logging.debug(f"Raw AI Output: {final_output_str}")

                                if hasattr(crew, 'tasks_outputs') and crew.tasks_outputs:
                                    oracle_logic_summary = crew.tasks_outputs[0]
                                else:
                                    oracle_logic_summary = "No summary available."

                                clean_sql = ""
                                if "```sql" in final_output_str:
                                    clean_sql = re.search(r"```sql\s*(.*?)\s*```", final_output_str, re.DOTALL).group(1)
                                elif "```" in final_output_str:
                                    clean_sql = re.search(r"```\s*(.*?)\s*```", final_output_str, re.DOTALL).group(1)
                                else:
                                    clean_sql = final_output_str.strip()
                                
                                clean_sql = clean_sql.encode('utf-8').decode('unicode_escape')
                                logging.info("Decoded literal escape sequences.")
                                logging.debug(f"Cleaned and decoded SQL:\n{clean_sql}")

                                converted_sql = convert_oracle_to_snowflake(clean_sql)
                                wrapped_sql = wrap_sql_in_dbt_model(converted_sql, model_type)

                                summary_path = create_summary_file(log_dir, file.name, wrapped_sql, model_type, oracle_logic_summary)
                                
                                status.update(label="✅ **Migration complete!**", state="complete", expanded=False)
                            except Exception as e:
                                logging.critical(f"CrewAI execution failed with an exception: {e}")
                                status.update(label="❌ **Migration failed.**", state="error", expanded=False)
                                st.error(f"❌ CrewAI execution failed: {e}")
                                continue
                    
                    # Moved outside of the 'with st.status' block
                    if summary_path:
                        st.markdown("### Migration Summary")
                        with open(summary_path, "r") as f:
                            st.text_area("Summary Report", f.read(), height=400)
