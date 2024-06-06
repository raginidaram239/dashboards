import openai

# Set up OpenAI API key
api_key = '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'  # Replace with your actual API key

# Function to test API key
def test_api_key():
    client = openai.OpenAI(api_key=api_key)  # Instantiate the client with the API key
    
    try:
        response = client.chat.completions.create(  # Use v1/chat/completions endpoint for chat models
            model="gpt-3.5-turbo",  # Updated to a supported chat model
            messages=[{"role": "user", "content": "Hello world"}]  # Example message
        )
        print("API Key is valid.")
        print("Response from API:", response.choices[0].content.strip())  # Access text content using 'content' attribute
    except Exception as e:
        print("An error occurred:", e)

# Run the test
test_api_key()
