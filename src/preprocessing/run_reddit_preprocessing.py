#!/usr/bin/env python3
"""
Convenience script to run Reddit corpus preprocessing with correct paths.

This script automatically sets up the correct paths for the PropInsight project
and runs the Reddit corpus preprocessor with Singlish and property domain integration.

Usage:
    python run_reddit_preprocessing.py
"""

import os
import sys
import subprocess
from pathlib import Path

def main():
    # Get the project root directory
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    
    # Define paths
    input_file = project_root / "data" / "processed" / "reddit_corpus_2023_2025.csv"
    output_file = project_root / "data" / "processed" / "reddit_corpus_enhanced.csv"
    singlish_corpus = project_root / "data" / "corpus" / "Singlish"
    property_corpus = project_root / "data" / "corpus" / "SGPropertyDomain"
    report_file = project_root / "results" / "reddit_preprocessing_report.json"
    
    # Ensure output directories exist
    output_file.parent.mkdir(parents=True, exist_ok=True)
    report_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Check if input file exists
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        print("Please ensure the Reddit corpus CSV file exists at the specified location.")
        sys.exit(1)
    
    # Check if corpus directories exist
    if not singlish_corpus.exists():
        print(f"Error: Singlish corpus directory not found: {singlish_corpus}")
        sys.exit(1)
    
    if not property_corpus.exists():
        print(f"Error: Property corpus directory not found: {property_corpus}")
        sys.exit(1)
    
    # Build command
    preprocessor_script = script_dir / "reddit_corpus_preprocessor.py"
    cmd = [
        sys.executable,
        str(preprocessor_script),
        "--input", str(input_file),
        "--output", str(output_file),
        "--singlish-corpus", str(singlish_corpus),
        "--property-corpus", str(property_corpus),
        "--report", str(report_file)
    ]
    
    print("Starting Reddit corpus preprocessing...")
    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    print(f"Singlish corpus: {singlish_corpus}")
    print(f"Property corpus: {property_corpus}")
    print(f"Report: {report_file}")
    print()
    
    # Run the preprocessor
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("Preprocessing completed successfully!")
        print(result.stdout)
        
        if result.stderr:
            print("Warnings/Errors:")
            print(result.stderr)
            
    except subprocess.CalledProcessError as e:
        print(f"Error running preprocessor: {e}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print(f"Error: Could not find the preprocessor script: {preprocessor_script}")
        sys.exit(1)

if __name__ == "__main__":
    main()