import streamlit as st
import os
import datetime
import base64
from functools import partial

st.title("Dynamic Airflow DAG Generator")

# --- Input Section ---
st.header("DAG Configuration")
dag_name = st.text_input("DAG Name", "my_dynamic_dag")
dag_owner_id = st.text_input("DAG Owner Email", "you@example.com")
dag_start_dt_input = st.text_input("DAG Start Date (YYYY-MM-DD)", '2025-02-25')
cron_schedule = st.text_input("Cron Schedule", '30 6 * * *')
user_emails_input = st.text_input("Additional Email Recipients (comma-separated)")
user_emails = [email.strip() for email in user_emails_input.split(',') if email.strip()]
email_ids = [dag_owner_id] + [email for email in user_emails if email != dag_owner_id]

# --- File Upload Section ---
st.header("Upload SQL Files")
uploaded_files = st.file_uploader("Upload your SQL files", type="sql", accept_multiple_files=True)

# --- Paths ---
basepath_default = 'sql-files'
template_path_default = 'dag_template.py'
output_dir_default = 'generated-dags'

basepath = st.text_input("SQL Files Directory", basepath_default)
template_path = st.text_input("DAG Template File Path", template_path_default)
output_dir = st.text_input("Output DAG Directory", output_dir_default)

if st.button("Generate DAG"):
    if not uploaded_files:
        st.error("Please upload at least one SQL file.")
    elif not os.path.exists(template_path):
        st.error(f"Template file not found at: {template_path}")
    else:
        try:
            # Parse date
            dag_start_dt = datetime.datetime.strptime(dag_start_dt_input, "%Y-%m-%d")

            # Read the Template
            with open(template_path, 'r') as f:
                template_content = f.read()

            # Replace placeholders
            formatted_emails = ", ".join([f'"{email.strip()}"' for email in email_ids])
            email_callback = f'partial(email.send_mail_slack, [{formatted_emails}])'

            updated_content = template_content.replace("DAG_OWNER = ''", f'DAG_OWNER = "{dag_owner_id}"')
            updated_content = updated_content.replace(
                "dag_start_dt = datetime(2023, 10, 7)",
                f"dag_start_dt = datetime({dag_start_dt.year}, {dag_start_dt.month}, {dag_start_dt.day})"
            )
            updated_content = updated_content.replace("'dag_name'", f"'{dag_name}'")
            updated_content = updated_content.replace(
                "schedule_interval='here cron_schedule will come'",
                f"schedule_interval='{cron_schedule}'"
            )
            updated_content = updated_content.replace(
                "partial(email.send_mail_slack, [listof emails, here one email id with be the value inside DAG_OWNER field])",
                email_callback
            )

            # Load SQL files and generate tasks
            sql_blocks = []
            presto_tasks = []
            dependency_chain_tasks = []

            os.makedirs(basepath, exist_ok=True)
            for i, uploaded_file in enumerate(uploaded_files):
                file_path = os.path.join(basepath, uploaded_file.name)
                try:
                    with open(file_path, 'wb') as f:
                        f.write(uploaded_file.getbuffer())

                    sql_var = f"sql{i+1}"
                    task_var = f"presto_task{i+1}"

                    # Read SQL content
                    with open(file_path, 'r') as sql_file:
                        content = sql_file.read().strip()

                    # SQL variable
                    sql_blocks.append(f'\n{sql_var} = """{content}"""\n\n')

                    # Presto task
                    presto_tasks.append(
                        f"""{task_var} = PythonOperator(
    task_id='ExecutePrestoQuery{i+1}',
    python_callable=presto.execute_presto_query_cli,
    op_kwargs={{'query': {sql_var}, 'user': DAG_OWNER}},
    dag=dag,
)\n\n"""
                    )

                    # Dependency tracking
                    dependency_chain_tasks.append(task_var)

                except Exception as e:
                    st.error(f"Error processing file {uploaded_file.name}: {e}")
                    break
            else:
                # Define task dependencies
                dependency_chain = " >> ".join(dependency_chain_tasks)

                # Final DAG content
                final_dag_content = updated_content + "\n\n" + "".join(sql_blocks) + "".join(presto_tasks) + dependency_chain + "\n"

                # Save DAG to file
                os.makedirs(output_dir, exist_ok=True)
                output_filename = os.path.join(output_dir, f"{dag_name}.py")
                try:
                    with open(output_filename, 'w') as f:
                        f.write(final_dag_content)

                    st.success(f"DAG generated and saved as: {output_filename}")

                    # ✅ Download link
                    b64 = base64.b64encode(final_dag_content.encode()).decode()
                    href = f'<a href="data:file/text;base64,{b64}" download="{dag_name}.py">📥 Download {dag_name}.py</a>'
                    st.markdown(href, unsafe_allow_html=True)

                    # ✅ Optional: Show contents
                    st.text_area("Generated DAG File Content", final_dag_content, height=400)

                except Exception as e:
                    st.error(f"Error saving DAG file: {e}")

        except ValueError:
            st.error("Invalid date format for DAG Start Date. Please use YYYY-MM-DD.")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
