import openai

# Set up OpenAI API key
openai.api_key = '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'

# Example feedback data
feedback_data = [
    "Driver John Doe was very polite and helpful. Customer Jane Smith appreciated his service.",
    "Driver Mike Johnson was on time. Customer Emily Davis was satisfied with the ride.",
    "Driver Alex Brown went out of his way to help. Customer Chris Lee was impressed by his dedication."
]

# Function to extract names
def extract_names(feedback_data):
    extracted_info = []
    
    for feedback in feedback_data:
        prompt = f"Extract the driver names and customer names from the following feedback: {feedback}"
        
        response = openai.Completion.create(
            engine="text-davinci-003",
            prompt=prompt,
            max_tokens=100
        )
        
        extracted_info.append(response.choices[0].text.strip())
    
    return extracted_info

# Extract the information
extracted_info = extract_names(feedback_data)

# Print the extracted information
for info in extracted_info:
    print(info)
