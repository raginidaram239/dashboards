import os
import streamlit as st
import pandas as pd
import openai
import databutton as db

from utils import get_data
from app_brain import handle_openai_query

def display_app_header():
    st.title("1️⃣ One-Prompt Charts 📊 ")
    st.markdown("***Prompt about your data, and see it visualized** ✨ This app runs on the power of your prompting. As here in Databutton HQ, we envision, '**Prompting is the new programming.**'*")

display_app_header()

with st.expander("App Overview", expanded=False):
    st.markdown(
        """
        You will find each function either in the library or in the main script. Feel free to modify it according to your needs.
        """
    )

API = st.text_input("Enter Your Open API key", type="password")

if API:
    os.environ["OPENAI_API_KEY"] = API
    openai.api_key = API  # Set the API key for OpenAI

    options = st.radio(
        "Data Usage", options=["Upload file", "Use Data in Storage"], horizontal=True
    )
    
    if options == "Upload file":
        df = get_data()
    else:
        df = db.storage.dataframes.get(key="spectra-csv")

    if df is not None:
        with st.expander("Show data"):
            st.write(df)

        column_names = ", ".join(df.columns)

        if not df.empty:
            handle_openai_query(df, column_names)
        else:
            st.warning("The given data is empty.")
    else:
        st.warning("No data loaded.")
else:
    st.warning("Please enter your OpenAI API key.")
