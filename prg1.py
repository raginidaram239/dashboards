import streamlit as st
import pandas as pd
import plotly.express as px

# Set page config to wide mode
st.set_page_config(layout="wide")

# Load and preprocess data 
def load_data():
    data = pd.read_csv('UN_food_security.csv')
    item_filter = "Prevalence of severe food insecurity in the total population (percent) (3-year average)"
    filtered_data = data[data['Item'] == item_filter]
    filtered_data['Year_Middle'] = filtered_data['Year'].apply(lambda x: int(x.split('-')[0]) + 1)
    filtered_data['Value_Clean'] = pd.to_numeric(filtered_data['Value'].str.replace('<', '').replace('>', ''), errors='coerce')
    filtered_data.dropna(subset=['Value_Clean'], inplace=True)
    return filtered_data

data = load_data()

# Streamlit app
st.title('Global Food Security Dashboard')

# Dropdown menu for year selection
years = data['Year_Middle'].unique()
selected_year = st.selectbox('Select Year', years)

# Filter data for the selected year
filtered_data_year = data[data['Year_Middle'] == selected_year]

# Choropleth map on its own row
fig_map = px.choropleth(filtered_data_year,
                        locations="Area",
                        locationmode="country names",
                        color="Value_Clean",
                        hover_name="Area",
                        hover_data={"Year_Middle": False, "Value_Clean": True},
                        color_continuous_scale="YlOrRd",
                        title="Global Food Insecurity")
st.plotly_chart(fig_map, use_container_width=True)

# Horizontal bar chart on its own row, below the map
top_countries = filtered_data_year.nlargest(10, 'Value_Clean')
fig_bar = px.bar(top_countries,
                 x='Value_Clean',
                 y='Area',
                 orientation='h',
                 color='Value_Clean',
                 color_continuous_scale="YlOrRd",
                 title="Top Countries by Food Insecurity Level")
fig_bar.update_layout(yaxis={'categoryorder': 'total ascending'})
st.plotly_chart(fig_bar, use_container_width=True)
