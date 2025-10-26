"""
Singapore Real Estate Data Labeling System
==========================================

This script provides comprehensive labeling for Singapore real estate data including:
- Property-specific sentiment analysis
- Singlish and cultural context understanding
- Policy impact classification
- Singapore real estate domain expertise
- Location impact analysis
- Aspect-Based Sentiment Analysis (ABSA)
- Named Entity Recognition (NER)

Author: AI Assistant (Singapore Real Estate Expert)
Date: 2025
"""

import pandas as pd
import numpy as np
import json
import re
import os
import time
import random
from datetime import datetime
from typing import Dict, List, Tuple, Any
from dotenv import load_dotenv
import openai
from dataclasses import dataclass
import logging
from openai import OpenAI
import argparse
import tiktoken

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class LabelingResult:
    """Data class to store labeling results"""
    property_sentiment: Dict[str, Any]
    singlish_cultural_context: Dict[str, Any]
    policy_impact: Dict[str, Any]
    location_impact: Dict[str, Any]
    aspect_based_sentiment: Dict[str, Any]
    named_entities: Dict[str, Any]
    overall_analysis: Dict[str, Any]
    domain_expertise: Dict[str, Any]

class SingaporeRealEstateLabeler:
    """
    Comprehensive labeling system for Singapore real estate data
    """
    
    def __init__(self, model: str | None = None, max_tokens: int = 1000, reasoning_effort: str = "medium", verbosity: str = "medium"):
        """Initialize the labeler with OpenAI API and configuration"""
        # Let OpenAI SDK read environment variables automatically for better security
        self.client = OpenAI().with_options(timeout=60)
        
        # Model configuration (allow override via arg or env var)
        self.model = model or os.getenv("OPENAI_MODEL") or "gpt-4o"
        self.max_tokens = max_tokens
        
        # GPT-5 specific parameters
        self.reasoning_effort = reasoning_effort  # minimal, low, medium, high
        self.verbosity = verbosity  # low, medium, high
        
        # Initialize tokenizer for proper token counting
        try:
            self.tokenizer = tiktoken.encoding_for_model(self.model)
        except KeyError:
            # Fallback to a common encoding if model not found
            self.tokenizer = tiktoken.get_encoding("cl100k_base")
        
        # Singapore-specific real estate knowledge base
        self.singapore_locations = {
            'districts': [
                'District 1', 'District 2', 'District 3', 'District 4', 'District 5',
                'District 6', 'District 7', 'District 8', 'District 9', 'District 10',
                'District 11', 'District 12', 'District 13', 'District 14', 'District 15',
                'District 16', 'District 17', 'District 18', 'District 19', 'District 20',
                'District 21', 'District 22', 'District 23', 'District 24', 'District 25',
                'District 26', 'District 27', 'District 28'
            ],
            'areas': [
                'Orchard', 'Marina Bay', 'Sentosa', 'Jurong', 'Woodlands', 'Tampines',
                'Bedok', 'Toa Payoh', 'Bishan', 'Ang Mo Kio', 'Hougang', 'Punggol',
                'Sengkang', 'Yishun', 'Choa Chu Kang', 'Bukit Batok', 'Bukit Panjang',
                'Clementi', 'Queenstown', 'Tiong Bahru', 'Chinatown', 'Little India',
                'Bugis', 'Clarke Quay', 'Robertson Quay', 'Dhoby Ghaut', 'Somerset',
                'Novena', 'Newton', 'Bukit Timah', 'Holland Village', 'Tanglin',
                'River Valley', 'Tanjong Pagar', 'Raffles Place', 'Boat Quay',
                'East Coast', 'Katong', 'Joo Chiat', 'Geylang', 'Kallang',
                'Lavender', 'Rochor', 'Balestier', 'Serangoon', 'Potong Pasir',
                'Toa Payoh', 'Braddell', 'Marymount', 'Thomson', 'Sin Ming',
                'Mayflower', 'Whitley', 'Dunearn', 'Sixth Avenue', 'Farrer Road',
                'Telok Blangah', 'Harbourfront', 'Redhill', 'Tiong Bahru',
                'Outram Park', 'Tanjong Katong', 'Mountbatten', 'Stadium',
                'Nicoll Highway', 'Esplanade', 'Promenade', 'Bayfront',
                'Marina South', 'Shenton Way', 'Anson Road', 'Cecil Street'
            ],
            'mrt_lines': [
                'North South Line', 'East West Line', 'Circle Line', 'North East Line',
                'Downtown Line', 'Thomson-East Coast Line', 'Cross Island Line'
            ]
        }
        
        self.property_types = [
            'HDB', 'Condominium', 'Private Apartment', 'Landed Property',
            'Executive Condominium', 'DBSS', 'Shophouse', 'Commercial',
            'Industrial', 'Mixed Development', 'Integrated Development'
        ]
        
        self.singlish_terms = [
            'lah', 'lor', 'meh', 'sia', 'hor', 'leh', 'mah', 'wah',
            'aiyo', 'alamak', 'shiok', 'steady', 'power', 'solid',
            'chope', 'kiasu', 'kiasi', 'bojio', 'paiseh', 'sian',
            'buay tahan', 'confirm plus chop', 'die die must',
            'good good', 'can can', 'cannot make it', 'blur like sotong'
        ]
        
        self.policy_keywords = [
            'cooling measures', 'ABSD', 'Additional Buyer\'s Stamp Duty',
            'SSD', 'Seller\'s Stamp Duty', 'TDSR', 'Total Debt Servicing Ratio',
            'LTV', 'Loan-to-Value', 'MOP', 'Minimum Occupation Period',
            'BTO', 'Build-to-Order', 'SBF', 'Sale of Balance Flats',
            'CORENET X', 'URA', 'Urban Redevelopment Authority',
            'HDB', 'Housing Development Board', 'BCA', 'Building and Construction Authority',
            'MAS', 'Monetary Authority of Singapore', 'MND', 'Ministry of National Development'
        ]

    def _is_gpt5_model(self) -> bool:
        """Check if the current model is a GPT-5 variant"""
        if not self.model:
            return False
        model_lower = self.model.lower()
        # Check for various GPT-5 model names and variants
        gpt5_patterns = ['gpt-5']
        return any(pattern in model_lower for pattern in gpt5_patterns)

    def analyze_with_gpt(self, text: str, analysis_type: str) -> Dict[str, Any]:
        """
        Use GPT model to analyze text based on analysis type
        """
        prompts = {
            'property_sentiment': f"""
            As a Singapore real estate expert, analyze the sentiment of this text specifically related to property and real estate matters.
            
            Text: {text}
            
            Provide analysis in JSON format with:
            1. overall_sentiment: positive/negative/neutral (with confidence score 0-1)
            2. property_market_sentiment: bullish/bearish/neutral (with confidence score 0-1)
            3. investor_sentiment: optimistic/pessimistic/neutral (with confidence score 0-1)
            4. key_sentiment_drivers: list of phrases that drive the sentiment
            5. market_implications: potential impact on property market
            6. sentiment_intensity: scale of 1-10
            """,
            
            'singlish_cultural': f"""
            As a Singapore cultural and linguistic expert, analyze this text for Singlish usage and cultural context.
            
            Text: {text}
            
            Provide analysis in JSON format with:
            1. singlish_detected: true/false
            2. singlish_terms: list of Singlish words/phrases found
            3. cultural_references: Singapore-specific cultural references
            4. local_context_score: 0-10 (how locally relevant)
            5. formality_level: formal/semi-formal/informal/colloquial
            6. target_audience: locals/expats/general/government
            7. cultural_nuances: important cultural context to understand
            """,
            
            'policy_impact': f"""
            As a Singapore real estate policy expert, analyze the policy implications of this text.
            
            Text: {text}
            
            Provide analysis in JSON format with:
            1. policy_relevance: high/medium/low
            2. policy_type: regulatory/fiscal/monetary/planning/other
            3. affected_segments: list of property segments affected
            4. impact_timeline: immediate/short-term/medium-term/long-term
            5. impact_magnitude: major/moderate/minor
            6. stakeholders_affected: list of affected parties
            7. policy_sentiment: supportive/restrictive/neutral
            8. compliance_requirements: any new requirements mentioned
            """,
            
            'location_impact': f"""
            As a Singapore real estate location expert, analyze the location-specific impacts mentioned in this text.
            
            Text: {text}
            
            Provide analysis in JSON format with:
            1. locations_mentioned: list of specific locations/areas
            2. location_sentiment: positive/negative/neutral for each location
            3. development_impact: new developments or changes mentioned
            4. accessibility_impact: transport/connectivity changes
            5. amenity_impact: new amenities or facilities
            6. district_analysis: which districts are affected
            7. property_value_implications: potential impact on property values
            8. investment_attractiveness: how locations' investment appeal changes
            """,
            
            'aspect_sentiment': f"""
            As a Singapore real estate analyst, perform aspect-based sentiment analysis on this text.
            
            Text: {text}
            
            Analyze sentiment for these aspects and provide in JSON format:
            1. price_affordability: sentiment and key phrases
            2. market_supply: sentiment and key phrases  
            3. demand_trends: sentiment and key phrases
            4. government_policies: sentiment and key phrases
            5. infrastructure_development: sentiment and key phrases
            6. economic_factors: sentiment and key phrases
            7. foreign_investment: sentiment and key phrases
            8. construction_industry: sentiment and key phrases
            9. residential_market: sentiment and key phrases
            10. commercial_market: sentiment and key phrases
            
            For each aspect include: sentiment (positive/negative/neutral), confidence (0-1), key_phrases (list)
            """,
            
            'named_entities': f"""
            As a Singapore real estate expert, extract and classify named entities from this text.
            
            Text: {text}
            
            Provide analysis in JSON format with:
            1. organizations: government agencies, companies, institutions
            2. locations: areas, districts, buildings, developments
            3. persons: ministers, officials, industry leaders
            4. policies: specific policies, schemes, regulations
            5. projects: development projects, infrastructure projects
            6. financial_figures: amounts, percentages, financial data
            7. dates: important dates and timelines
            8. property_types: types of properties mentioned
            9. technical_terms: industry-specific terminology
            10. legal_entities: acts, regulations, standards
            """,
            
            'domain_expertise': f"""
            As a Singapore real estate domain expert, provide a domain-specific assessment of the text.
            
            Text: {text}
            
            Provide analysis in JSON format with:
            1. expertise_assessment: concise expert summary of the situation
            2. market_context: relevant Singapore market context and trends
            3. risk_factors: key risks and mitigations
            4. opportunities: investment or development opportunities
            5. recommendations: actionable expert recommendations
            6. confidence: 0-1 confidence score
            """
        }
        
        max_attempts = 3
        backoff_seconds = [5, 10, 20]
        last_error = None
        for attempt in range(max_attempts):
            try:
                if self._is_gpt5_model():
                    # Use GPT-5 responses API with reasoning and verbosity controls
                    response = self.client.responses.create(
                        model=self.model,
                        input=prompts[analysis_type],
                        reasoning={"effort": self.reasoning_effort},
                        text={"verbosity": self.verbosity}
                    )
                    result_text = response.output_text
                else:
                    # Use traditional chat completions API for non-GPT-5 models
                    kwargs = {
                        "model": self.model,
                        "messages": [
                            {"role": "system", "content": "You are an expert Singapore real estate analyst with deep knowledge of the local market, policies, culture, and language. Always respond with valid JSON format."},
                            {"role": "user", "content": prompts[analysis_type]}
                        ],
                        "max_tokens": self.max_tokens
                    }
                    response = self.client.chat.completions.create(**kwargs)
                    result_text = response.choices[0].message.content

                # Some models may wrap JSON in code fences; strip them if present
                if result_text.strip().startswith("```"):
                    result_text = result_text.strip().strip("`")
                result = json.loads(result_text)
                return result
            except Exception as e:
                last_error = e
                logger.warning(f"Attempt {attempt+1}/{max_attempts} failed for {analysis_type}: {str(e)}")
                if attempt < max_attempts - 1:
                    time.sleep(backoff_seconds[attempt])
                else:
                    logger.error(f"Error in GPT analysis for {analysis_type}: {str(e)}")
                    return {"error": str(e), "analysis_type": analysis_type}

    def label_single_record(self, record: pd.Series) -> LabelingResult:
        """
        Label a single record with all analysis types
        """
        text = record['cleaned_text']
        title = record['title']
        combined_text = f"Title: {title}\n\nContent: {text}"
        # Clamp overly long inputs to reduce token/time issues
        if len(combined_text) > 12000:
            combined_text = combined_text[:12000]
        
        logger.info(f"Labeling record: {record['id']}")
        
        # Perform all analyses
        property_sentiment = self.analyze_with_gpt(combined_text, 'property_sentiment')
        singlish_cultural = self.analyze_with_gpt(combined_text, 'singlish_cultural')
        policy_impact = self.analyze_with_gpt(combined_text, 'policy_impact')
        location_impact = self.analyze_with_gpt(combined_text, 'location_impact')
        aspect_sentiment = self.analyze_with_gpt(combined_text, 'aspect_sentiment')
        named_entities = self.analyze_with_gpt(combined_text, 'named_entities')
        domain_expertise = self.analyze_with_gpt(combined_text, 'domain_expertise')
        
        # Overall analysis combining all insights
        overall_analysis = {
            'processing_timestamp': datetime.now().isoformat(),
            'text_length': len(combined_text),
            'source': record['source'],
            'url': record['url'],
            'language': record['language'],
            'analysis_confidence': self._calculate_overall_confidence([
                property_sentiment, singlish_cultural, policy_impact,
                location_impact, aspect_sentiment, named_entities, domain_expertise
            ])
        }
        
        return LabelingResult(
            property_sentiment=property_sentiment,
            singlish_cultural_context=singlish_cultural,
            policy_impact=policy_impact,
            location_impact=location_impact,
            aspect_based_sentiment=aspect_sentiment,
            named_entities=named_entities,
            overall_analysis=overall_analysis,
            domain_expertise=domain_expertise
        )

    def _calculate_overall_confidence(self, analyses: List[Dict]) -> float:
        """Calculate overall confidence score from all analyses"""
        confidence_scores = []
        
        for analysis in analyses:
            if 'error' not in analysis:
                # Extract confidence scores from various fields
                for key, value in analysis.items():
                    if isinstance(value, dict) and 'confidence' in value:
                        confidence_scores.append(value['confidence'])
                    elif 'confidence' in str(key).lower() and isinstance(value, (int, float)):
                        confidence_scores.append(value)
        
        return np.mean(confidence_scores) if confidence_scores else 0.5

    def process_csv_file(self, input_file: str, output_dir: str, max_records: int | None = None) -> str:
        """
        Process entire CSV file and save labeled results
        """
        logger.info(f"Processing file: {input_file}")
        
        # Read input data
        df = pd.read_csv(input_file)
        logger.info(f"Loaded {len(df)} records")
        
        if max_records is not None:
            df = df.head(max_records)
            logger.info(f"Limiting processing to first {len(df)} records due to max_records={max_records}")
        
        # Prepare results storage
        labeled_results = []
        
        # Process each record
        for idx, record in df.iterrows():
            try:
                result = self.label_single_record(record)
                
                # Create labeled record
                labeled_record = {
                    # Original data
                    'id': record['id'],
                    'source': record['source'],
                    'timestamp': record['timestamp'],
                    'url': record['url'],
                    'language': record['language'],
                    'title': record['title'],
                    'original_length': record['original_length'],
                    'cleaned_length': record['cleaned_length'],
                    'cleaned_text': record['cleaned_text'],
                    
                    # Labeling results
                    'property_sentiment': json.dumps(result.property_sentiment),
                    'singlish_cultural_context': json.dumps(result.singlish_cultural_context),
                    'policy_impact': json.dumps(result.policy_impact),
                    'location_impact': json.dumps(result.location_impact),
                    'aspect_based_sentiment': json.dumps(result.aspect_based_sentiment),
                    'named_entities': json.dumps(result.named_entities),
                    'domain_expertise': json.dumps(result.domain_expertise),
                    'overall_analysis': json.dumps(result.overall_analysis),
                    
                    # Processing metadata
                    'labeling_timestamp': datetime.now().isoformat(),
                    'labeling_version': '1.0'
                }
                
                labeled_results.append(labeled_record)
                logger.info(f"Completed labeling for record {idx + 1}/{len(df)}")
                
            except Exception as e:
                logger.error(f"Error processing record {record['id']}: {str(e)}")
                continue
        
        # Save results
        if labeled_results:
            results_df = pd.DataFrame(labeled_results)
            
            # Generate output filename
            input_filename = os.path.basename(input_file)
            output_filename = input_filename.replace('.csv', '_labeled.csv')
            output_path = os.path.join(output_dir, output_filename)
            
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            # Save to CSV
            results_df.to_csv(output_path, index=False)
            logger.info(f"Saved {len(results_df)} labeled records to: {output_path}")
            
            # Also save a summary report
            self._generate_summary_report(results_df, output_dir, input_filename)
            
            return output_path
        else:
            logger.error("No records were successfully processed")
            return None

    def _generate_summary_report(self, df: pd.DataFrame, output_dir: str, input_filename: str):
        """Generate a summary report of the labeling results"""
        
        summary = {
            'processing_summary': {
                'input_file': input_filename,
                'total_records': len(df),
                'processing_date': datetime.now().isoformat(),
                'labeling_version': '1.0'
            },
            'sentiment_distribution': {},
            'policy_impact_summary': {},
            'location_analysis': {},
            'cultural_context_summary': {}
        }
        
        # Analyze sentiment distribution
        sentiment_counts = {'positive': 0, 'negative': 0, 'neutral': 0}
        policy_relevance = {'high': 0, 'medium': 0, 'low': 0}
        
        for _, row in df.iterrows():
            try:
                # Property sentiment analysis
                prop_sentiment = json.loads(row['property_sentiment'])
                if 'overall_sentiment' in prop_sentiment:
                    sentiment = prop_sentiment['overall_sentiment']
                    if isinstance(sentiment, str):
                        sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
                
                # Policy impact analysis
                policy_data = json.loads(row['policy_impact'])
                if 'policy_relevance' in policy_data:
                    relevance = policy_data['policy_relevance']
                    policy_relevance[relevance] = policy_relevance.get(relevance, 0) + 1
                    
            except Exception as e:
                logger.warning(f"Error analyzing summary for record: {str(e)}")
                continue
        
        summary['sentiment_distribution'] = sentiment_counts
        summary['policy_impact_summary'] = policy_relevance
        
        # Save summary report
        summary_filename = input_filename.replace('.csv', '_labeling_summary.json')
        summary_path = os.path.join(output_dir, summary_filename)
        
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Summary report saved to: {summary_path}")

def main():
    """Main function to run the labeling process"""
    
    parser = argparse.ArgumentParser(description="Singapore Real Estate Labeling Script")
    parser.add_argument("--input_file", type=str, default=r"d:\Semester4Project\PropInsight\data\processed\bca\bca_media_releases_20250921_173345_processed.csv", help="Input CSV file to label")
    parser.add_argument("--output_dir", type=str, default=r"d:\Semester4Project\PropInsight\data\labeled", help="Directory to save labeled output")
    parser.add_argument("--max_records", type=int, default=None, help="Limit the number of records to process for testing")
    parser.add_argument("--model", type=str, default=os.getenv("OPENAI_MODEL"), help="OpenAI model to use (overrides env)")
    parser.add_argument("--max_tokens", type=int, default=5000, help="Max tokens in the completion")
    parser.add_argument("--reasoning_effort", type=str, default="high", choices=["minimal", "low", "medium", "high"], help="GPT-5 reasoning effort level")
    parser.add_argument("--verbosity", type=str, default="high", choices=["low", "medium", "high"], help="GPT-5 response verbosity level")
    args = parser.parse_args()

    # Initialize labeler
    labeler = SingaporeRealEstateLabeler(model=args.model, max_tokens=args.max_tokens, reasoning_effort=args.reasoning_effort, verbosity=args.verbosity)
    
    # Define file paths
    input_file = args.input_file
    output_dir = args.output_dir
    
    # Process the file
    try:
        output_path = labeler.process_csv_file(input_file, output_dir, max_records=args.max_records)
        if output_path:
            print(f"✅ Labeling completed successfully!")
            print(f"📁 Output file: {output_path}")
            print(f"📊 Check the summary report in the same directory")
        else:
            print("❌ Labeling failed - no records processed")
            
    except Exception as e:
        print(f"❌ Error during labeling process: {str(e)}")
        logger.error(f"Main process error: {str(e)}")

if __name__ == "__main__":
    main()