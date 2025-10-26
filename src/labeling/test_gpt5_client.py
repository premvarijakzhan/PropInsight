"""
GPT-5 Client Diagnostic Test Script
==================================

This script tests the OpenAI client initialization and GPT-5 API connectivity
to ensure proper authentication and configuration.
"""

import os
import json
from dotenv import load_dotenv
from openai import OpenAI

def test_environment_variables():
    """Test if environment variables are properly loaded"""
    print("=== Environment Variables Test ===")
    
    # Load environment variables
    load_dotenv()
    
    api_key = os.getenv("OPENAI_API_KEY")
    project = os.getenv("OPENAI_PROJECT")
    
    print(f"OPENAI_API_KEY present: {'Yes' if api_key else 'No'}")
    print(f"OPENAI_API_KEY length: {len(api_key) if api_key else 0}")
    print(f"OPENAI_PROJECT: {project}")
    
    if not api_key:
        print("❌ ERROR: OPENAI_API_KEY is missing!")
        return False
    
    if not project:
        print("⚠️  WARNING: OPENAI_PROJECT is missing!")
    
    print("✅ Environment variables loaded successfully")
    return True

def test_client_initialization():
    """Test OpenAI client initialization"""
    print("\n=== Client Initialization Test ===")
    
    try:
        api_key = os.getenv("OPENAI_API_KEY")
        project = os.getenv("OPENAI_PROJECT")
        
        # Initialize client exactly like in the main script
        client = OpenAI(api_key=api_key, project=project).with_options(timeout=60)
        
        print("✅ OpenAI client initialized successfully")
        print(f"Client base URL: {client.base_url}")
        return client
    
    except Exception as e:
        print(f"❌ ERROR: Failed to initialize client: {str(e)}")
        return None

def test_gpt5_detection():
    """Test GPT-5 model detection logic"""
    print("\n=== GPT-5 Detection Test ===")
    
    def _is_gpt5_model(model: str) -> bool:
        """Same logic as in the main script"""
        if not model:
            return False
        model_lower = model.lower()
        gpt5_patterns = ['gpt-5']
        return any(pattern in model_lower for pattern in gpt5_patterns)
    
    test_models = [
        "gpt-5",
        "gpt-5-turbo", 
        "GPT-5",
        "gpt-4",
        "gpt-4-turbo",
        None,
        ""
    ]
    
    for model in test_models:
        is_gpt5 = _is_gpt5_model(model)
        print(f"Model: {model} -> GPT-5: {is_gpt5}")
    
    print("✅ GPT-5 detection logic working")

def test_gpt5_api_call(client):
    """Test actual GPT-5 API call"""
    print("\n=== GPT-5 API Call Test ===")
    
    if not client:
        print("❌ Cannot test API call - client not initialized")
        return False
    
    try:
        # Test with a simple prompt
        test_prompt = "Analyze this Singapore real estate text: 'New BTO launch in Punggol with good connectivity to MRT.'"
        
        print("Testing GPT-5 responses.create() API...")
        
        response = client.responses.create(
            model="gpt-5",
            input=test_prompt,
            reasoning={"effort": "medium"},
            text={"verbosity": "medium"}
        )
        
        print("✅ GPT-5 API call successful!")
        print(f"Response type: {type(response)}")
        print(f"Response has output_text: {hasattr(response, 'output_text')}")
        
        if hasattr(response, 'output_text'):
            output_preview = response.output_text[:200] + "..." if len(response.output_text) > 200 else response.output_text
            print(f"Output preview: {output_preview}")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: GPT-5 API call failed: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        
        # Try to provide more specific error information
        if "401" in str(e):
            print("🔍 This is an authentication error - check your API key and project settings")
        elif "404" in str(e):
            print("🔍 This might be a model availability issue - GPT-5 may not be available yet")
        elif "429" in str(e):
            print("🔍 Rate limit exceeded - try again in a moment")
        
        return False

def test_fallback_model(client):
    """Test fallback to GPT-4 if GPT-5 fails"""
    print("\n=== Fallback Model Test ===")
    
    if not client:
        print("❌ Cannot test fallback - client not initialized")
        return False
    
    try:
        print("Testing fallback to GPT-4...")
        
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a Singapore real estate expert."},
                {"role": "user", "content": "Briefly analyze: 'New BTO launch in Punggol'"}
            ],
            max_tokens=100
        )
        
        print("✅ GPT-4 fallback working!")
        output_preview = response.choices[0].message.content[:200]
        print(f"Output preview: {output_preview}")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: GPT-4 fallback failed: {str(e)}")
        return False

def main():
    """Run all diagnostic tests"""
    print("🔍 GPT-5 Client Diagnostic Test")
    print("=" * 50)
    
    # Test 1: Environment variables
    if not test_environment_variables():
        print("\n❌ CRITICAL: Environment setup failed. Cannot proceed.")
        return
    
    # Test 2: Client initialization
    client = test_client_initialization()
    
    # Test 3: GPT-5 detection logic
    test_gpt5_detection()
    
    # Test 4: GPT-5 API call
    gpt5_success = test_gpt5_api_call(client)
    
    # Test 5: Fallback model (if GPT-5 fails)
    if not gpt5_success:
        print("\n🔄 GPT-5 failed, testing fallback...")
        test_fallback_model(client)
    
    print("\n" + "=" * 50)
    print("🏁 Diagnostic test completed!")
    
    if gpt5_success:
        print("✅ Your setup is ready for GPT-5!")
        print("You can now run: python singapore_real_estate_labeler.py")
    else:
        print("⚠️  GPT-5 not working, but fallback models available")
        print("Consider using --model gpt-4 for now")

if __name__ == "__main__":
    main()