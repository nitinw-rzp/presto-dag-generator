import streamlit as st
import os
import datetime
import base64
from functools import partial

st.title("Dynamic Airflow DAG Generator")

# --- Custom CSS ---
st.markdown(
    """
    <style>
    .stDownloadButton, .stButton>button {
        background-color: rgb(92, 146, 246) !important;
        color: white !important;
        padding: 10px 24px !important;
        border-radius: 5px !important;
        font-size: 18px !important;
        margin: 4px 2px !important;
        cursor: pointer !important;
        transition: background-color 0.3s ease !important;
    }
    .stDownloadButton:hover, .stButton>button:hover {
        background-color: #4CAF50 !important;
    }
    .stDownloadButton {
        text-decoration: none !important;
    }
    .bottom-right-text {
        position: fixed;
        bottom: 10px;
        right: 10px;
        font-size: 0.9em;
        color: #888;
    }
    .razorpay-blue {
        color: #3366FF;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- DAG Configuration ---
st.header("DAG Configuration")
dag_name = st.text_input("DAG Name", "my_dynamic_dag")
dag_owner_id = st.text_input("DAG Owner Email", "you@example.com")
dag_start_dt_input = st.text_input("DAG Start Date (YYYY-MM-DD)", '2025-02-25')
cron_schedule = st.text_input("Cron Schedule", '30 6 * * *')
user_emails_input = st.text_input("Additional Email Recipients (comma-separated)")
user_emails = [email.strip() for email in user_emails_input.split(',') if email.strip()]
email_ids = [dag_owner_id] + [email for email in user_emails if email != dag_owner_id]

# --- Sensor Configuration ---
sensor_required = st.checkbox("Is Sensor Required?")
if sensor_required:
    external_dag_id = st.text_input("External DAG ID", "")
    execution_delta_mins = st.number_input("Execution Delta (in minutes)", min_value=0, value=75)

# --- SQL Upload ---
st.header("Upload SQL Files in Order")
num_files = st.number_input("How many SQL files do you want to upload?", min_value=1, max_value=20, step=1)
uploaded_files_map = {}
for i in range(1, num_files + 1):
    uploaded_file = st.file_uploader(f"Upload SQL File #{i}", type="sql", key=f"sql_file_{i}")
    if uploaded_file:
        uploaded_files_map[f"file_{i}"] = uploaded_file

# --- Dependency Flow Selection ---
st.subheader("Task Dependency Flow")
flow_type = st.radio("Select flow type", ["Sequential", "Parallel", "Mixed"], index=0)

# --- Task Identification ---
task_name_map = {}
task_order_options = []

if sensor_required:
    sensor_task_id = f"{external_dag_id}_trigger"
    task_name_map[sensor_task_id] = sensor_task_id
    task_order_options.append(sensor_task_id)

for i in range(1, num_files + 1):
    task_id = f"ExecutePrestoQuery{i}"
    task_var = f"presto_task{i}"
    task_name_map[task_id] = task_var
    task_order_options.append(task_id)

# --- Custom Task Order ---
custom_task_order = []
st.subheader("Define Custom Execution Order")
used = set()
for step in range(len(task_order_options)):
    remaining = [t for t in task_order_options if t not in used]
    if flow_type == "Mixed":
        selected = st.multiselect(f"Step {step + 1}", remaining, key=f"step_{step}")
        if selected:
            custom_task_order.append(selected if len(selected) > 1 else selected[0])
            used.update(selected)
    elif flow_type == "Sequential":
        selected = st.selectbox(f"Step {step + 1}", remaining, key=f"step_{step}")
        custom_task_order.append(selected)
        used.add(selected)

# --- Paths ---
basepath_default = 'sql-files'
template_path_default = 'dag_template.py'
output_dir_default = 'generated-dags'

basepath = st.text_input("SQL Files Directory", basepath_default)
template_path = st.text_input("DAG Template File Path", template_path_default)
output_dir = st.text_input("Output DAG Directory", output_dir_default)

if st.button("Generate DAG"):
    if len(uploaded_files_map) != num_files:
        st.error("Please upload all the SQL files in the order specified.")
    elif not os.path.exists(template_path):
        st.error(f"Template file not found at: {template_path}")
    elif sensor_required and not external_dag_id:
        st.error("Please fill in External DAG ID.")
    else:
        try:
            dag_start_dt = datetime.datetime.strptime(dag_start_dt_input, "%Y-%m-%d")

            with open(template_path, 'r') as f:
                template_content = f.read()

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

            if sensor_required:
                updated_content = updated_content.replace(
                    "from datetime import datetime, timedelta",
                    "from datetime import datetime, timedelta\nfrom airflow.sensors.external_task import ExternalTaskSensor"
                )

            sql_blocks = []
            presto_tasks = []
            os.makedirs(basepath, exist_ok=True)

            for i, (key, uploaded_file) in enumerate(uploaded_files_map.items()):
                file_path = os.path.join(basepath, uploaded_file.name)
                with open(file_path, 'wb') as f:
                    f.write(uploaded_file.getbuffer())

                sql_var = f"sql{i+1}"
                task_var = f"presto_task{i+1}"

                with open(file_path, 'r') as sql_file:
                    content = sql_file.read().strip()

                sql_blocks.append(f'\n{sql_var} = """{content}"""\n\n')
                presto_tasks.append(f"""{task_var} = PythonOperator(
    task_id='ExecutePrestoQuery{i+1}',
    python_callable=presto.execute_presto_query_cli,
    op_kwargs={{'query': {sql_var}, 'user': DAG_OWNER}},
    dag=dag,
)\n\n""")

            sensor_code = ""
            if sensor_required:
                hours = execution_delta_mins // 60
                mins = execution_delta_mins % 60
                sensor_code = f"""
{external_dag_id}_trigger = ExternalTaskSensor(
    task_id='{external_dag_id}_trigger',
    external_dag_id='{external_dag_id}',
    external_task_id=None,
    allowed_states=['success'],
    failed_states=['failed'],
    execution_delta = timedelta(hours={hours}, minutes={mins}),
    dag=dag,
)\n\n"""

            if flow_type == "Parallel":
                dependency_chain = f"[{', '.join([task_name_map[t] for t in task_order_options])}]"
            else:
                chain_parts = []
                for step in custom_task_order:
                    if isinstance(step, list):
                        chain_parts.append(f"[{', '.join([task_name_map[t] for t in step])}]")
                    else:
                        chain_parts.append(task_name_map[step])
                dependency_chain = " >> ".join(chain_parts)

            final_dag_content = updated_content + "\n\n" + sensor_code + "".join(sql_blocks) + "".join(presto_tasks) + dependency_chain + "\n"

            os.makedirs(output_dir, exist_ok=True)
            output_filename = os.path.join(output_dir, f"{dag_name}.py")

            with open(output_filename, 'w') as f:
                f.write(final_dag_content)

            st.success(f"DAG generated and saved as: {output_filename}")

            b64 = base64.b64encode(final_dag_content.encode()).decode()
            href = f'<a class="stDownloadButton" href="data:file/text;base64,{b64}" download="{dag_name}.py">📥 Download {dag_name}.py</a>'
            st.markdown(href, unsafe_allow_html=True)

            st.text_area("Generated DAG File Content", final_dag_content, height=400)

        except ValueError:
            st.error("Invalid date format for DAG Start Date. Please use YYYY-MM-DD.")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")

st.markdown(
    """
    <div class="bottom-right-text">
        DSE Team @<span class="razorpay-blue">Razorpay</span>
    </div>
    """,
    unsafe_allow_html=True,
)
