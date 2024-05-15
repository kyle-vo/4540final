import json
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import plotly.graph_objs as go
import plotly.express as pl

def load_data():
    all_pay = {}
    all_get = {}

    table_names = ['ancestor_currency', 'affliction_currency']
    for table_name in table_names:
        file_path = f'/storage/{table_name}_analysis.json'
        with open(file_path, 'r') as f:
            data = json.load(f)
            averages = data['averages']
            
            pay_chaos_orb_values = {}
            get_chaos_orb_values = {}
            
            for key, value in averages.items():
                get_currency, pay_currency = key.split(', ')
                if get_currency == "Chaos Orb":
                    if round(value, 2) != 0:
                        pay_chaos_orb_values[pay_currency] = round(value, 2)
                elif pay_currency == "Chaos Orb":
                    # Get Chaos Orb
                    if round(value, 2) != 0:
                        get_chaos_orb_values[get_currency] = round(value, 2)
            
            all_pay[table_name] = pay_chaos_orb_values
            all_get[table_name] = get_chaos_orb_values
    # st.write(all_pay_chaos_orb_values)
    return all_pay, all_get

def descending(chaos_orb_values, dataset, title):
    sorted_values = dict(sorted(chaos_orb_values.items(), key=lambda item: item[1], reverse=True))
    currencies = list(sorted_values.keys())
    values = list(sorted_values.values())
    
    trace = go.Bar(x=currencies, y=values)
    layout = go.Layout(title=title, xaxis=dict(title='Currency'), yaxis=dict(title='Average Value'), hovermode='closest')
    fig = go.Figure(data=[trace], layout=layout)
    fig.update_layout(yaxis=dict(range=[0, 25]))
    
    st.plotly_chart(fig)
    
def regular_pay(all_pay, dataset):
    df = pd.DataFrame(all_pay[dataset].items(), columns=['Currency', 'Average Value'])
    fig = pl.line(df, x='Currency', y='Average Value', title=f"Alphabetical Average Pay Values for {dataset}")
    st.plotly_chart(fig)
    
def regular_get(all_get, dataset):
    df = pd.DataFrame(all_get[dataset].items(), columns=['Currency', 'Average Value'])
    fig = pl.line(df, x='Currency', y='Average Value', title=f"Alphabetical Average Get Values for {dataset}")
    st.plotly_chart(fig)
    
st.set_option('deprecation.showPyplotGlobalUse', False)

all_pay, all_get = load_data()

selected_dataset = st.selectbox("Select Dataset", ["All"] + list(all_pay.keys()))

# Plot Pay Chaos Orbs
if selected_dataset == "All":
    for dataset, pay_chaos_orb_values in all_pay.items():
        descending(pay_chaos_orb_values, dataset, f"Descending Average Pay Values for {dataset}")
        regular_pay(all_pay, dataset)
else:
    descending(all_pay[selected_dataset], selected_dataset, f"Descending Average Pay Values for {selected_dataset}")
    regular_pay(all_pay, selected_dataset)

# Plot Get Chaos Orbs
if selected_dataset == "All":
    for dataset, get_chaos_orb_values in all_get.items():
        descending(get_chaos_orb_values, dataset, f"Descending Average Get Values for {dataset}")
        regular_pay(all_get, dataset)
else:
    descending(all_get[selected_dataset], selected_dataset, f"Descending Average Get Values for {selected_dataset}")
    regular_get(all_get, selected_dataset)

# Plot best % profit from earliest to current
def calculate_percentage(all_pay, all_get):
    all_pay_avg = {}
    all_get_avg = {}
    for dataset, pay_chaos_orb_values in all_pay.items():
        for currency, average_value in pay_chaos_orb_values.items():
            if currency not in all_pay_avg:
                all_pay_avg[currency] = {}
            all_pay_avg[currency][dataset] = average_value

    for dataset, get_chaos_orb_values in all_get.items():
        for currency, average_value in get_chaos_orb_values.items():
            if currency not in all_get_avg:
                all_get_avg[currency] = {}
            all_get_avg[currency][dataset] = average_value

    df_pay = pd.DataFrame(all_pay_avg).transpose()
    df_get = pd.DataFrame(all_get_avg).transpose()

    first_pay_dataset = df_pay.iloc[:, 0]
    last_pay_dataset = df_pay.iloc[:, -1]
    percentage_change_pay = (last_pay_dataset / first_pay_dataset) * 100
    earning_amounts_pay = (last_pay_dataset - first_pay_dataset)

    first_get_dataset = df_get.iloc[:, 0]
    last_get_dataset = df_get.iloc[:, -1]
    percentage_change_get = (last_get_dataset / first_get_dataset) * 100
    earning_amounts_get = (last_get_dataset - first_get_dataset)

    return percentage_change_pay, earning_amounts_pay, percentage_change_get, earning_amounts_get

def profit(top_currencies, percentage_change, earning, title):
    fig = go.Figure(data=[
        go.Bar(x=top_currencies, y=percentage_change, 
               text=[f"{change:.2f}% / Earn: {earning_amount:.2f}" for change, earning_amount in zip(percentage_change, earning)], 
               textposition='auto', hoverinfo='text', marker_color='green')
    ])
    fig.update_layout(title=title, xaxis=dict(title='Currency'), yaxis=dict(title='Percentage Change'))
    st.plotly_chart(fig)
    
percentage_change_pay, earning_pay, percentage_change_get, earning_get = calculate_percentage(all_pay, all_get)
top_pay = percentage_change_pay.nlargest(10).index
top_get = percentage_change_get.nlargest(10).index
profit(top_pay, percentage_change_pay[top_pay], earning_pay[top_pay],'Top Pay % Profit on Average')
profit(top_get, percentage_change_get[top_get], earning_get[top_get],'Top Get % Profit on Average')