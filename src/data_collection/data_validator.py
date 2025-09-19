"""
PropInsight Data Validator

This module provides validation utilities for ensuring scraped data conforms to
the standardized JSON Lines format defined in data_format_specification.md.

Key features:
1. Schema validation for all data sources
2. Data quality scoring
3. Duplicate detection
4. Content filtering and sanitization
5. Batch validation for large datasets

Reasoning: Data validation is critical for maintaining data quality and ensuring
consistent format across all scrapers. This prevents downstream processing issues
and maintains the integrity of our analysis pipeline.
"""

import json
import re
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass
from pathlib import Path
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Result of data validation"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    quality_score: float
    processing_flags: List[str]

class DataValidator:
    """
    Comprehensive data validator for PropInsight scraped data
    
    Validates data against the standardized schema and calculates quality scores
    """
    
    def __init__(self):
        """Initialize validator with schema definitions"""
        self.required_fields = {
            'id', 'source', 'scraped_at', 'url', 'title', 'content',
            'metadata', 'sentiment', 'rating', 'location', 'quality_score', 'processing_flags'
        }
        
        self.valid_sources = {'reddit', 'government', 'propertyguru', 'hardwarezone', 'renotalk', 'edgeprop_srx', '99co'}
        self.valid_sentiments = {'positive', 'negative', 'neutral'}
        self.valid_rating_methods = {'explicit', 'sentiment', 'keyword', 'hybrid'}
        self.valid_confidences = {'high', 'medium', 'low'}
        
        # Singapore property keywords for content validation
        self.property_keywords = {
            'types': ['hdb', 'condo', 'landed', 'bto', 'resale', 'private', 'ec', 'dbss'],
            'areas': ['orchard', 'marina bay', 'sentosa', 'punggol', 'sengkang', 'tampines',
                     'jurong', 'woodlands', 'yishun', 'ang mo kio', 'bishan', 'toa payoh',
                     'queenstown', 'bukit timah', 'novena', 'dhoby ghaut', 'raffles place'],
            'terms': ['property', 'housing', 'real estate', 'mortgage', 'cpf', 'downpayment',
                     'valuation', 'psf', 'sqft', 'bedroom', 'bathroom', 'balcony', 'parking']
        }
        
        # Content quality filters
        self.spam_patterns = [
            r'click here', r'buy now', r'limited time', r'act fast',
            r'guaranteed', r'risk free', r'no obligation'
        ]
        
        # Duplicate detection cache
        self.content_hashes: Set[str] = set()
    
    def validate_record(self, record: Dict[str, Any]) -> ValidationResult:
        """
        Validate a single data record
        
        Args:
            record: Dictionary containing scraped data
            
        Returns:
            ValidationResult with validation status and quality metrics
        """
        errors = []
        warnings = []
        processing_flags = []
        
        # Check required fields
        missing_fields = self.required_fields - set(record.keys())
        if missing_fields:
            errors.append(f"Missing required fields: {missing_fields}")
        
        # Validate field types and values
        if 'source' in record:
            if record['source'] not in self.valid_sources:
                errors.append(f"Invalid source: {record['source']}")
        
        # Validate ID format
        if 'id' in record:
            if not isinstance(record['id'], str) or len(record['id']) < 5:
                errors.append("ID must be a string with at least 5 characters")
        
        # Validate URL
        if 'url' in record:
            if not self._is_valid_url(record['url']):
                errors.append(f"Invalid URL format: {record['url']}")
        
        # Validate datetime format
        if 'scraped_at' in record:
            if not self._is_valid_datetime(record['scraped_at']):
                errors.append(f"Invalid datetime format: {record['scraped_at']}")
        
        # Validate content
        if 'content' in record:
            content_validation = self._validate_content(record['content'])
            if content_validation['errors']:
                errors.extend(content_validation['errors'])
            if content_validation['warnings']:
                warnings.extend(content_validation['warnings'])
            processing_flags.extend(content_validation['flags'])
        
        # Validate sentiment
        if 'sentiment' in record:
            sentiment_validation = self._validate_sentiment(record['sentiment'])
            if sentiment_validation['errors']:
                errors.extend(sentiment_validation['errors'])
            processing_flags.extend(sentiment_validation['flags'])
        
        # Validate rating
        if 'rating' in record:
            rating_validation = self._validate_rating(record['rating'])
            if rating_validation['errors']:
                errors.extend(rating_validation['errors'])
            processing_flags.extend(rating_validation['flags'])
        
        # Validate location
        if 'location' in record:
            location_validation = self._validate_location(record['location'])
            if location_validation['warnings']:
                warnings.extend(location_validation['warnings'])
            processing_flags.extend(location_validation['flags'])
        
        # Check for duplicates
        if self._is_duplicate_content(record.get('content', '')):
            warnings.append("Potential duplicate content detected")
            processing_flags.append('potential_duplicate')
        else:
            processing_flags.append('duplicate_check_passed')
        
        # Calculate quality score
        quality_score = self._calculate_quality_score(record, processing_flags)
        
        # Add quality-based flags
        if quality_score >= 0.8:
            processing_flags.append('high_quality')
        elif quality_score >= 0.6:
            processing_flags.append('medium_quality')
        else:
            processing_flags.append('low_quality')
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            quality_score=quality_score,
            processing_flags=processing_flags
        )
    
    def _is_valid_url(self, url: str) -> bool:
        """Validate URL format"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False
    
    def _is_valid_datetime(self, dt_str: str) -> bool:
        """Validate ISO 8601 datetime format"""
        try:
            datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            return True
        except:
            return False
    
    def _validate_content(self, content: str) -> Dict[str, List[str]]:
        """
        Validate content quality and extract flags
        
        Args:
            content: Text content to validate
            
        Returns:
            Dictionary with errors, warnings, and flags
        """
        errors = []
        warnings = []
        flags = []
        
        if not isinstance(content, str):
            errors.append("Content must be a string")
            return {'errors': errors, 'warnings': warnings, 'flags': flags}
        
        # Check minimum length
        if len(content) < 20:
            warnings.append("Content is very short (< 20 characters)")
            flags.append('short_content')
        elif len(content) >= 200:
            flags.append('sufficient_length')
        
        # Check for spam patterns
        content_lower = content.lower()
        for pattern in self.spam_patterns:
            if re.search(pattern, content_lower):
                warnings.append(f"Potential spam pattern detected: {pattern}")
                flags.append('potential_spam')
                break
        else:
            flags.append('content_filtered')
        
        # Check for Singapore property keywords
        has_property_keywords = False
        for category, keywords in self.property_keywords.items():
            for keyword in keywords:
                if keyword in content_lower:
                    has_property_keywords = True
                    break
            if has_property_keywords:
                break
        
        if has_property_keywords:
            flags.append('singapore_property_keywords')
        else:
            warnings.append("No Singapore property keywords detected")
        
        # Check language (basic English detection)
        english_words = ['the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by']
        english_word_count = sum(1 for word in english_words if word in content_lower.split())
        
        if english_word_count >= 2:
            flags.append('english_content')
        else:
            # Could be Singlish or other languages, which is fine for Singapore context
            flags.append('non_standard_english')
        
        return {'errors': errors, 'warnings': warnings, 'flags': flags}
    
    def _validate_sentiment(self, sentiment: Dict[str, Any]) -> Dict[str, List[str]]:
        """Validate sentiment data structure"""
        errors = []
        flags = []
        
        required_sentiment_fields = {'score', 'confidence', 'label'}
        missing_fields = required_sentiment_fields - set(sentiment.keys())
        if missing_fields:
            errors.append(f"Missing sentiment fields: {missing_fields}")
        
        # Validate sentiment score
        if 'score' in sentiment:
            score = sentiment['score']
            if not isinstance(score, (int, float)) or not (-1.0 <= score <= 1.0):
                errors.append("Sentiment score must be a number between -1.0 and 1.0")
        
        # Validate confidence
        if 'confidence' in sentiment:
            confidence = sentiment['confidence']
            if not isinstance(confidence, (int, float)) or not (0.0 <= confidence <= 1.0):
                errors.append("Sentiment confidence must be a number between 0.0 and 1.0")
            else:
                if confidence >= 0.8:
                    flags.append('high_sentiment_confidence')
                elif confidence >= 0.5:
                    flags.append('medium_sentiment_confidence')
                else:
                    flags.append('low_sentiment_confidence')
        
        # Validate label
        if 'label' in sentiment:
            if sentiment['label'] not in self.valid_sentiments:
                errors.append(f"Invalid sentiment label: {sentiment['label']}")
        
        if not errors:
            flags.append('sentiment_analyzed')
        
        return {'errors': errors, 'warnings': [], 'flags': flags}
    
    def _validate_rating(self, rating: Dict[str, Any]) -> Dict[str, List[str]]:
        """Validate rating data structure"""
        errors = []
        flags = []
        
        required_rating_fields = {'explicit', 'inferred', 'confidence', 'method'}
        missing_fields = required_rating_fields - set(rating.keys())
        if missing_fields:
            errors.append(f"Missing rating fields: {missing_fields}")
        
        # Validate explicit rating
        if 'explicit' in rating and rating['explicit'] is not None:
            explicit = rating['explicit']
            if not isinstance(explicit, (int, float)) or not (1.0 <= explicit <= 5.0):
                errors.append("Explicit rating must be a number between 1.0 and 5.0")
            else:
                flags.append('explicit_rating_available')
        
        # Validate inferred rating
        if 'inferred' in rating and rating['inferred'] is not None:
            inferred = rating['inferred']
            if not isinstance(inferred, (int, float)) or not (1.0 <= inferred <= 5.0):
                errors.append("Inferred rating must be a number between 1.0 and 5.0")
            else:
                flags.append('rating_inferred')
        
        # Validate confidence
        if 'confidence' in rating:
            if rating['confidence'] not in self.valid_confidences:
                errors.append(f"Invalid rating confidence: {rating['confidence']}")
        
        # Validate method
        if 'method' in rating:
            if rating['method'] not in self.valid_rating_methods:
                errors.append(f"Invalid rating method: {rating['method']}")
        
        return {'errors': errors, 'warnings': [], 'flags': flags}
    
    def _validate_location(self, location: Dict[str, Any]) -> Dict[str, List[str]]:
        """Validate location data structure"""
        warnings = []
        flags = []
        
        # Check for mentioned areas
        if 'mentioned_areas' in location and location['mentioned_areas']:
            if isinstance(location['mentioned_areas'], list) and len(location['mentioned_areas']) > 0:
                flags.append('location_extracted')
                
                # Check if areas are Singapore-specific
                singapore_areas = any(area.lower() in [kw.lower() for kw in self.property_keywords['areas']] 
                                    for area in location['mentioned_areas'])
                if singapore_areas:
                    flags.append('singapore_location')
            else:
                warnings.append("Mentioned areas should be a non-empty list")
        else:
            warnings.append("No location information extracted")
        
        # Check property types
        if 'property_types' in location and location['property_types']:
            if isinstance(location['property_types'], list) and len(location['property_types']) > 0:
                flags.append('property_type_identified')
        
        # Check coordinates
        if 'coordinates' in location and location['coordinates']:
            coords = location['coordinates']
            if isinstance(coords, list) and len(coords) == 2:
                lat, lng = coords
                # Singapore coordinates roughly: 1.2-1.5 lat, 103.6-104.0 lng
                if 1.0 <= lat <= 1.6 and 103.0 <= lng <= 104.5:
                    flags.append('singapore_coordinates')
                else:
                    warnings.append("Coordinates appear to be outside Singapore")
            else:
                warnings.append("Invalid coordinate format")
        
        return {'errors': [], 'warnings': warnings, 'flags': flags}
    
    def _is_duplicate_content(self, content: str) -> bool:
        """
        Check if content is a duplicate using content hashing
        
        Args:
            content: Text content to check
            
        Returns:
            True if content is likely a duplicate
        """
        if not content or len(content) < 50:
            return False
        
        # Create hash of normalized content
        normalized_content = re.sub(r'\s+', ' ', content.lower().strip())
        content_hash = hashlib.md5(normalized_content.encode()).hexdigest()
        
        if content_hash in self.content_hashes:
            return True
        
        self.content_hashes.add(content_hash)
        return False
    
    def _calculate_quality_score(self, record: Dict[str, Any], flags: List[str]) -> float:
        """
        Calculate overall quality score for a record
        
        Args:
            record: Data record
            flags: Processing flags
            
        Returns:
            Quality score between 0.0 and 1.0
        """
        score = 0.0
        
        # Content length (20% weight)
        content = record.get('content', '')
        if len(content) >= 200:
            score += 0.2
        elif len(content) >= 50:
            score += 0.1
        
        # Source reliability (30% weight)
        source = record.get('source', '')
        source_scores = {
            'government': 0.3,
            'propertyguru': 0.27,
            'reddit': 0.21,
            'hardwarezone': 0.18
        }
        score += source_scores.get(source, 0.0)
        
        # Sentiment confidence (20% weight)
        sentiment = record.get('sentiment', {})
        confidence = sentiment.get('confidence', 0.0)
        if confidence >= 0.8:
            score += 0.2
        elif confidence >= 0.5:
            score += 0.14
        else:
            score += 0.06
        
        # Location specificity (15% weight)
        if 'singapore_location' in flags:
            score += 0.15
        elif 'location_extracted' in flags:
            score += 0.075
        
        # Temporal relevance (15% weight)
        scraped_at = record.get('scraped_at', '')
        if scraped_at:
            try:
                scraped_date = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
                days_old = (datetime.now(timezone.utc) - scraped_date).days
                
                if days_old <= 180:  # 6 months
                    score += 0.15
                elif days_old <= 365:  # 1 year
                    score += 0.12
                elif days_old <= 730:  # 2 years
                    score += 0.09
                else:
                    score += 0.045
            except:
                score += 0.045  # Default for invalid dates
        
        return min(score, 1.0)  # Cap at 1.0
    
    def validate_batch(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate a batch of records and provide summary statistics
        
        Args:
            records: List of data records to validate
            
        Returns:
            Batch validation summary
        """
        results = []
        for record in records:
            results.append(self.validate_record(record))
        
        # Calculate summary statistics
        valid_count = sum(1 for r in results if r.is_valid)
        total_errors = sum(len(r.errors) for r in results)
        total_warnings = sum(len(r.warnings) for r in results)
        avg_quality = sum(r.quality_score for r in results) / len(results) if results else 0.0
        
        # Collect all flags
        all_flags = {}
        for result in results:
            for flag in result.processing_flags:
                all_flags[flag] = all_flags.get(flag, 0) + 1
        
        return {
            'total_records': len(records),
            'valid_records': valid_count,
            'invalid_records': len(records) - valid_count,
            'validation_rate': valid_count / len(records) if records else 0.0,
            'total_errors': total_errors,
            'total_warnings': total_warnings,
            'average_quality_score': avg_quality,
            'flag_distribution': all_flags,
            'detailed_results': results
        }
    
    def validate_jsonl_file(self, file_path: str) -> Dict[str, Any]:
        """
        Validate an entire JSONL file
        
        Args:
            file_path: Path to JSONL file
            
        Returns:
            File validation summary
        """
        records = []
        line_errors = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        record = json.loads(line)
                        records.append(record)
                    except json.JSONDecodeError as e:
                        line_errors.append(f"Line {line_num}: Invalid JSON - {e}")
        
        except FileNotFoundError:
            return {'error': f"File not found: {file_path}"}
        except Exception as e:
            return {'error': f"Error reading file: {e}"}
        
        # Validate records
        batch_result = self.validate_batch(records)
        batch_result['file_path'] = file_path
        batch_result['json_parse_errors'] = line_errors
        
        return batch_result

def main():
    """Example usage of DataValidator"""
    validator = DataValidator()
    
    # Example record
    sample_record = {
        "id": "reddit_test_123",
        "source": "reddit",
        "scraped_at": "2024-01-15T10:30:00Z",
        "url": "https://reddit.com/r/singapore/comments/test/",
        "title": "Test property discussion",
        "content": "This is a test discussion about HDB prices in Punggol. The new BTO launch seems expensive.",
        "metadata": {"subreddit": "singapore"},
        "sentiment": {"score": -0.2, "confidence": 0.75, "label": "negative"},
        "rating": {"explicit": None, "inferred": 2.8, "confidence": "medium", "method": "sentiment"},
        "location": {"mentioned_areas": ["Punggol"], "property_types": ["HDB", "BTO"], "coordinates": None},
        "quality_score": 0.0,  # Will be calculated
        "processing_flags": []  # Will be populated
    }
    
    result = validator.validate_record(sample_record)
    print(f"Validation result: {result}")

if __name__ == "__main__":
    main()