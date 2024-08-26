import streamlit as st
import requests
import pandas as pd
import json
import time
from datetime import timedelta

# Set page layout to wide
st.set_page_config(layout="wide", page_title="Opera Cloud H&F Extractor and Discrepancy Checker")

# Define placeholder JSON for user guidance
placeholder_json = '''{
  "authentication": {
    "xapikey": "replace_with_your_xapikey",
    "clientId": "replace_with_your_clientId",
    "hostname": "replace_with_your_hostname",
    "password": "replace_with_your_password",
    "username": "replace_with_your_username",
    "clientSecret": "replace_with_your_clientSecret",
    "externalSystemId": "replace_with_your_externalSystemId"
  }
}'''

# Streamlit app layout
st.title('Opera Cloud H&F Extractor and Discrepancy Checker')

# JSON configuration input and Submit button
json_input = st.empty()  # Create an empty placeholder for dynamic layout management
json_config = st.text_area("Paste your configuration JSON here:", placeholder=placeholder_json, height=100)
submit_json = st.button('Submit JSON')

# Process and validate JSON when submitted
if submit_json:
    # Auto-add curly braces if missing
    if not json_config.strip().startswith('{'):
        json_config = '{' + json_config + '}'
    try:
        # Parse the provided JSON
        config_data = json.loads(json_config)
        st.session_state['config_data'] = config_data  # Store in session state if further processing is needed
        st.success("JSON loaded successfully!")
    except json.JSONDecodeError:
        st.error("Invalid JSON format. Please correct it and try again.")

# Display forms even before JSON input
col1, col2 = st.columns([2, 1])

with col1:
    # Attempt to use session state data if available, otherwise initialize empty
    auth_data = st.session_state.get('config_data', {}).get('authentication', {})
    x_app_key = st.text_input('X-App-Key', value=auth_data.get('xapikey', ''))
    client_id = st.text_input('Client ID', value=auth_data.get('clientId', ''))
    hostname = st.text_input('Hostname', value=auth_data.get('hostname', ''))
    password = st.text_input('Password', value=auth_data.get('password', ''), type='password')
    username = st.text_input('Username', value=auth_data.get('username', ''))
    client_secret = st.text_input('Client Secret', value=auth_data.get('clientSecret', ''), type='password')
    ext_system_code = st.text_input('External System Code', value=auth_data.get('externalSystemId', ''))

with col2:
    hotel_id = st.text_input('Hotel ID', key="hotel_id")
    start_date = st.date_input('Start Date', key="start_date")
    end_date = st.date_input('End Date', key="end_date")
    retrieve_button = st.button('Retrieve Data', key='retrieve')

def split_date_range(start_date, end_date, max_days=400):
    ranges = []
    current_start_date = start_date
    while current_start_date < end_date:
        current_end_date = min(current_start_date + timedelta(days=max_days - 1), end_date)
        ranges.append((current_start_date, current_end_date))
        current_start_date = current_end_date + timedelta(days=1)
    return ranges

def authenticate(host, x_key, client, secret, user, passw):
    url = f"{host}/oauth/v1/tokens"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'x-app-key': x_key,
        'Authorization': 'Basic ' + requests.auth._basic_auth_str(client, secret),
    }
    data = {
        'username': user,
        'password': passw,
        'grant_type': 'password'
    }
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        st.error(f'Authentication failed: {response.text}')
        return None

def start_async_process(token, host, x_key, h_id, ext_code, s_date, e_date):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
        'x-app-key': x_key,
        'x-hotelId': h_id
    }
    data = {
        "dateRangeStart": s_date.strftime("%Y-%m-%d"),
        "dateRangeEnd": e_date.strftime("%Y-%m-%d"),
        "roomTypes": [""]
    }
    url = f"{host}/inv/async/v1/externalSystems/{ext_code}/hotels/{h_id}/revenueInventoryStatistics"
    response = requests.post(url, json=data, headers=headers)
    if response.status_code == 202:
        return response.headers.get('Location')
    else:
        st.error(f"Failed to start asynchronous process: {response.status_code} - {response.text}")
        return None

def wait_for_data_ready(location_url, token, x_key, h_id):
    headers = {
        'Authorization': f'Bearer {token}',
        'x-app-key': x_key,
        'x-hotelId': h_id
    }
    while True:
        response = requests.head(location_url, headers=headers)
        if response.status_code == 201:
            return response.headers.get('Location')
        elif response.status_code in [200, 202, 404]:
            time.sleep(10)
        else:
            st.error(f"Error checking data readiness: {response.status_code} - {response.reason}")
            return None

def retrieve_data(location_url, token, x_key, h_id):
    headers = {
        'Authorization': f'Bearer {token}',
        'x-app-key': x_key,
        'x-hotelId': h_id
    }
    response = requests.get(location_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Failed to retrieve data: {response.status_code} - {response.reason}")
        return None

def create_comparison_table(api_data, csv_data):
    """Create a table comparing API data with CSV data and calculating variances and accuracy percentages."""
    
    # Calculate totals for API data
    total_rooms_sold_api = api_data['roomsSold'].sum()
    total_room_revenue_api = api_data['roomRevenue'].sum()
    
    # Calculate totals for CSV data
    total_rooms_sold_csv = csv_data['rn'].sum()
    total_room_revenue_csv = csv_data['revNet'].sum()
    
    # Calculate variances
    rn_variance = total_rooms_sold_csv - total_rooms_sold_api
    revenue_variance = total_room_revenue_csv - total_room_revenue_api
    
    # Calculate percentage discrepancies
    rn_accuracy_pct = ((total_rooms_sold_csv - rn_variance) / total_rooms_sold_csv) * 100 if total_rooms_sold_csv != 0 else 0
    revenue_accuracy_pct = ((total_room_revenue_csv - revenue_variance) / total_room_revenue_csv) * 100 if total_room_revenue_csv != 0 else 0

    # Create a DataFrame for display
    comparison_df = pd.DataFrame({
        'Metric': ['Rooms Sold', 'Room Revenue'],
        'API Data': [total_rooms_sold_api, total_room_revenue_api],
        'CSV Data': [total_rooms_sold_csv, total_room_revenue_csv],
        'Variance': [rn_variance, revenue_variance],
        'Accuracy (%)': [rn_accuracy_pct, revenue_accuracy_pct]
    })
    
    return comparison_df

# Retrieve data and store in session state
if retrieve_button and 'api_data_combined' not in st.session_state:
    with st.spinner('Processing... Please wait.'):
        token = authenticate(hostname, x_app_key, client_id, client_secret, username, password)
        if token:
            date_ranges = split_date_range(start_date, end_date)
            all_data = []
            for s_date, e_date in date_ranges:
                initial_location_url = start_async_process(token, hostname, x_app_key, hotel_id, ext_system_code, s_date, e_date)
                if initial_location_url:
                    final_location_url = wait_for_data_ready(initial_location_url, token, x_app_key, hotel_id)
                    if final_location_url:
                        data = retrieve_data(final_location_url, token, x_app_key, hotel_id)
                        if data:
                            all_data.append(data)

            if all_data:
                st.success("Data retrieved successfully!")
                # Combine all JSON responses into a single list and store in session state
                st.session_state['api_data_combined'] = [item for sublist in all_data for item in sublist]

# Display API data on the left side
col1, col2 = st.columns(2)

with col1:
    st.header("API Data")
    if 'api_data_combined' in st.session_state and st.session_state['api_data_combined']:
        api_data_combined = st.session_state['api_data_combined']
        st.dataframe(pd.DataFrame(api_data_combined))
    else:
        st.write("No API data retrieved yet. Please retrieve data first.")

# Display CSV upload and accuracy check on the right side
with col2:
    st.header('Discrepancy Checker')
    uploaded_csv_file = st.file_uploader("Choose a Daily Totals file (CSV)", type=['csv'])

    if uploaded_csv_file and 'api_data_combined' in st.session_state:
        # Read CSV data from uploaded file
        csv_df = pd.read_csv(uploaded_csv_file, delimiter=';')

        # Compare the data between the API response and the CSV file
        api_data_combined = st.session_state['api_data_combined']
        comparison_result_df = create_comparison_table(pd.DataFrame(api_data_combined), csv_df)

        # Display the comparison table
        if not comparison_result_df.empty:
            st.header("Comparison Table")
            st.dataframe(comparison_result_df)
        else:
            st.error("Data could not be processed. Please check the file formats and contents.")
    else:
        st.write("Please retrieve data and upload a CSV file to proceed.")
