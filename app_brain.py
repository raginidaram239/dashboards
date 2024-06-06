import streamlit as st
import pandas as pd
import re
import openai
import matplotlib.pyplot as plt

MODEL_NAME = "gpt-3.5-turbo"

def handle_openai_query(df, column_names):
    query = st.text_area(
        "Enter your Prompt:",
        placeholder="Prompt tips: Use plotting related keywords such as 'Plots' or 'Charts' or 'Subplots'. Prompts must be concise and clear, example 'Bar plot for the first ten rows.'",
    )

    if st.button("Get Answer"):
        if query and query.strip() != "":
            prompt_content = f"""
            The dataset is ALREADY loaded into a DataFrame named 'df'. DO NOT load the data again.
            
            The DataFrame has the following columns: {column_names}
            
            Use package Pandas and Matplotlib ONLY.
            Provide SINGLE CODE BLOCK with a solution using Pandas and Matplotlib plots in a single figure to address the following query:
            
            {query}

            - USE SINGLE CODE BLOCK with a solution. 
            - Do NOT EXPLAIN the code 
            - DO NOT COMMENT the code. 
            - ALWAYS WRAP UP THE CODE IN A SINGLE CODE BLOCK.
            """

            messages = [
                {
                    "role": "system",
                    "content": "You are a helpful Data Visualization assistant who gives a single block without explaining or commenting the code to plot. IF ANYTHING NOT ABOUT THE DATA, JUST politely respond that you don't know.",
                },
                {"role": "user", "content": prompt_content},
            ]

            with st.spinner("📟 *Prompting is the new programming*..."):
                response = openai.ChatCompletion.create(
                    model=MODEL_NAME, messages=messages
                )
                result = response.choices[0].message['content'].strip()
                st.code(result)
            execute_openai_code(result, df, query)

def extract_code_from_markdown(md_text):
    code_blocks = re.findall(r"```(python)?(.*?)```", md_text, re.DOTALL)
    code = "\n".join([block[1].strip() for block in code_blocks])
    return code

def execute_openai_code(response_text: str, df: pd.DataFrame, query):
    code = extract_code_from_markdown(response_text)

    if code:
        try:
            fig, ax = plt.subplots()
            exec(code, {'df': df, 'plt': plt, 'fig': fig, 'ax': ax})
            st.pyplot(fig)
        except Exception as e:
            st.error(f"📟 Apologies, failed to execute the code due to the error: {str(e)}")
            st.warning(
                """
                📟 Check the error message and the code executed above to investigate further.
                """
            )
            st.code(code)
