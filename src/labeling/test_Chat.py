from dotenv import load_dotenv
import os
from openai import OpenAI

# Load environment variables from .env file
load_dotenv()

# Retrieve API key and project ID from environment variables
api_key = os.getenv("OPENAI_API_KEY")
project_id = os.getenv("OPENAI_PROJECT")

# Instantiate the OpenAI client with api_key and project
client = OpenAI(
    api_key=api_key,
    project=project_id
)

# Example prompt and request for GPT-5 with "medium" verbosity
response = client.responses.create(
    model="gpt-5",
    input="Write a function that computes the Fibonacci sequence up to n in Python.",
    reasoning={"effort": "medium"},
    text={"verbosity": "medium"},
)

print(response.output_text)
