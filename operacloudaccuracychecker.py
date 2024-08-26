import streamlit as st
import requests
import pandas as pd
from io import BytesIO
import json
import time
from datetime import timedelta

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
json_config = json_input.text_area("Paste your configuration JSON here:", placeholder=placeholder_json, height=100)
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

def process_data(api_data, csv_file):
    # Check if 'revInvStats' key exists in the API response
    if 'revInvStats' not in api_data[0]:
        st.error("The key 'revInvStats' is not found in the API response. Please check the API response structure.")
        return pd.DataFrame()  # Return an empty DataFrame or handle this case as needed

    # Convert API JSON data to DataFrame
    api_df = pd.json_normalize(api_data, 'revInvStats')

    # Read CSV data from uploaded file
    csv_df = pd.read_csv(csv_file, delimiter=';')

    # Trim column names to remove potential leading/trailing whitespace
    api_df.columns = api_df.columns.str.strip()
    csv_df.columns = csv_df.columns.str.strip()
    
    # Convert date columns
    csv_df['arrivalDate'] = pd.to_datetime(csv_df['arrivalDate'])
    api_df['occupancyDate'] = pd.to_datetime(api_df['occupancyDate'])

    # Merge on date
    merged = pd.merge(csv_df[['arrivalDate', 'rn', 'revNet']],
                      api_df[['occupancyDate', 'roomsSold', 'roomRevenue']],
                      left_on='arrivalDate', right_on='occupancyDate', how='inner')
    merged.drop('occupancyDate', axis=1, inplace=True)
    merged.columns = ['Date', 'RN_Juyo', 'Revenue_Juyo', 'RN_HF', 'Revenue_HF']
    
    # Fill NaNs and calculate differences
    merged.fillna(0, inplace=True)
    merged['RN_Difference'] = merged['RN_Juyo'] - merged['RN_HF']
    merged['Revenue_Difference'] = round(merged['Revenue_Juyo'] - merged['Revenue_HF'], 2)
    
    return merged

# Check if the 'retrieve_button' was clicked and data isn't already stored
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
                # Combine all JSON responses into a single DataFrame and store in session state
                st.session_state['api_data_combined'] = [item for sublist in all_data for item in sublist['revInvStats']]

# If data is already retrieved or has been retrieved just now
if 'api_data_combined' in st.session_state:
    api_data_combined = st.session_state['api_data_combined']

    # Proceed to file upload for discrepancy checking
    st.header('Discrepancy Checker')
    uploaded_csv_file = st.file_uploader("Choose a Daily Totals file (CSV)", type=['csv'])

    if uploaded_csv_file:
        # Process and compare the data
        comparison_result = process_data(api_data_combined, uploaded_csv_file)

        if not comparison_result.empty:
            st.sidebar.header("Filters")
            show_discrepancies_only = st.sidebar.checkbox("Show Only Discrepancies", value=True)

            default_columns = ['Date', 'RN_Difference', 'Revenue_Difference']
            columns_to_show = st.sidebar.multiselect("Select columns to display", comparison_result.columns, default=default_columns)
            
            filtered_data = comparison_result.loc[:, columns_to_show]
            if show_discrepancies_only:
                filtered_data = filtered_data[(comparison_result['RN_Difference'] != 0) | (comparison_result['Revenue_Difference'] != 0)]

            # KPI calculations
            current_date = pd.Timestamp.now().normalize()
            past_data = comparison_result[comparison_result['Date'] < current_date]
            future_data = comparison_result[comparison_result['Date'] >= current_date]

            past_rn_discrepancy_abs = abs(abs(past_data['RN_Difference']).sum())
            past_revenue_discrepancy_abs = abs(abs(past_data['Revenue_Difference']).sum())
            past_rn_discrepancy_pct = abs(abs(past_data['RN_Difference']).sum()) / past_data['RN_HF'].sum() * 100
            past_revenue_discrepancy_pct = abs(abs(past_data['Revenue_Difference']).sum()) / past_data['Revenue_HF'].sum() * 100

            future_rn_discrepancy_abs = abs(abs(future_data['RN_Difference']).sum())
            future_revenue_discrepancy_abs = abs(abs(future_data['Revenue_Difference']).sum())
            future_rn_discrepancy_pct = abs(abs(future_data['RN_Difference']).sum()) / future_data['RN_HF'].sum() * 100
            future_revenue_discrepancy_pct = abs(abs(future_data['Revenue_Difference']).sum()) / future_data['Revenue_HF'].sum() * 100

            rn_only_discrepancies = (filtered_data['RN_Difference'] != 0) & (filtered_data['Revenue_Difference'] == 0)
            rev_only_discrepancies = (filtered_data['Revenue_Difference'] != 0) & (filtered_data['RN_Difference'] == 0)
            if rn_only_discrepancies.any():
                st.warning("Warning: There are Room Night discrepancies without corresponding Revenue discrepancies. Something may be off in the configuration or the logic of the code.")
            if rev_only_discrepancies.any():
                st.warning("Warning: There are Revenue discrepancies without corresponding Room Night discrepancies. Something may be off in the configuration or the logic of the code.")

            st.header(f"Accuracy Report")
            kpi_col1, kpi_col2 = st.columns(2)
            with kpi_col1:
                st.subheader("Past")
                st.metric("RN Accuracy (%)", f"{100-past_rn_discrepancy_pct:.2f}%")
                st.metric("Revenue Accuracy (%)", f"{100-past_revenue_discrepancy_pct:.2f}%")
                st.metric("RN Discrepancy (Absolute)", f"{past_rn_discrepancy_abs} RNs")
                st.metric("Revenue Discrepancy (Absolute)", f"{past_revenue_discrepancy_abs}")

            with kpi_col2:
                st.subheader("Future")
                st.metric("RN Accuracy (%)", f"{100-future_rn_discrepancy_pct:.2f}%")
                st.metric("Revenue Accuracy (%)", f"{100-future_revenue_discrepancy_pct:.2f}%")
                st.metric("RN Discrepancy (Absolute)", f"{future_rn_discrepancy_abs} RNs")
                st.metric("Revenue Discrepancy (Absolute)", f"{future_revenue_discrepancy_abs:.2f}")
            
            st.header("Detailed Report")
            formatted_data = filtered_data.style.format({
                'RN_Difference': "{:.0f}",
                'Revenue_Difference': "{:.2f}"
            }).applymap(lambda x: "background-color: yellow" if isinstance(x, (int, float)) and x != 0 else "", subset=['RN_Difference', 'Revenue_Difference'])
            st.dataframe(formatted_data)
        else:
            st.error("Data could not be processed. Please check the file formats and contents.")
else:
    st.write("Please retrieve data and upload a CSV file to proceed.")
