# Raw Data Format Specification

## Overview

This document defines the standardized JSON Lines (.jsonl) format used for storing raw scraped data across all PropInsight data sources. JSON Lines format was chosen for its streaming capabilities, fault tolerance, and processing efficiency.

## Why JSON Lines Format?

1. **Streaming Processing**: Each line is a valid JSON object, allowing for memory-efficient processing of large datasets
2. **Fault Tolerance**: If one line is corrupted, other lines remain intact
3. **Append-Friendly**: New data can be easily appended without rewriting entire files
4. **Tool Compatibility**: Widely supported by data processing tools (pandas, Apache Spark, etc.)
5. **Human Readable**: Easy to inspect and debug individual records

## Universal Data Schema

Every record across all sources follows this base schema:

```json
{
  "id": "unique_identifier",
  "source": "reddit|government|propertyguru|hardwarezone",
  "scraped_at": "2024-01-15T10:30:00Z",
  "url": "source_url",
  "title": "content_title",
  "content": "main_text_content",
  "metadata": {
    "source_specific_fields": "varies_by_source"
  },
  "sentiment": {
    "score": 0.75,
    "confidence": 0.85,
    "label": "positive|negative|neutral"
  },
  "rating": {
    "explicit": 4.5,
    "inferred": 3.8,
    "confidence": "high|medium|low",
    "method": "explicit|sentiment|keyword|hybrid"
  },
  "location": {
    "mentioned_areas": ["Orchard", "Marina Bay"],
    "property_types": ["condo", "HDB"],
    "coordinates": null
  },
  "quality_score": 0.82,
  "processing_flags": ["duplicate_check_passed", "content_filtered"]
}
```

## Source-Specific Schemas

### Reddit Data Format

```json
{
  "id": "reddit_post_1a2b3c",
  "source": "reddit",
  "scraped_at": "2024-01-15T10:30:00Z",
  "url": "https://reddit.com/r/singapore/comments/abc123/",
  "title": "Thoughts on new BTO launch in Punggol?",
  "content": "Just saw the new BTO launch... prices seem reasonable but location is quite ulu...",
  "metadata": {
    "subreddit": "singapore",
    "author": "user123",
    "created_utc": "2024-01-15T08:00:00Z",
    "score": 45,
    "num_comments": 23,
    "upvote_ratio": 0.87,
    "post_type": "submission",
    "flair": "Housing",
    "gilded": 0,
    "comments": [
      {
        "id": "comment_xyz",
        "author": "commenter1",
        "body": "I think the location is actually quite good...",
        "score": 12,
        "created_utc": "2024-01-15T09:15:00Z",
        "parent_id": "abc123"
      }
    ]
  },
  "sentiment": {
    "score": -0.2,
    "confidence": 0.75,
    "label": "negative"
  },
  "rating": {
    "explicit": null,
    "inferred": 2.8,
    "confidence": "medium",
    "method": "sentiment"
  },
  "location": {
    "mentioned_areas": ["Punggol"],
    "property_types": ["BTO", "HDB"],
    "coordinates": null
  },
  "quality_score": 0.78,
  "processing_flags": ["singapore_property_keywords", "sufficient_length"]
}
```

### Government Data Format

```json
{
  "id": "gov_mnd_20240115_001",
  "source": "government",
  "scraped_at": "2024-01-15T10:30:00Z",
  "url": "https://www.mnd.gov.sg/newsroom/press-releases/view/new-housing-policies-2024",
  "title": "New Housing Policies to Support First-Time Buyers",
  "content": "The Ministry of National Development announced new measures to help first-time homebuyers...",
  "metadata": {
    "agency": "MND",
    "publication_date": "2024-01-15T00:00:00Z",
    "category": "press-release",
    "tags": ["housing", "policy", "first-time-buyers"],
    "rss_feed": "https://www.mnd.gov.sg/rss/press-releases",
    "full_text_scraped": true,
    "word_count": 1250
  },
  "sentiment": {
    "score": 0.6,
    "confidence": 0.90,
    "label": "positive"
  },
  "rating": {
    "explicit": null,
    "inferred": 4.2,
    "confidence": "high",
    "method": "keyword"
  },
  "location": {
    "mentioned_areas": ["Singapore"],
    "property_types": ["HDB", "private"],
    "coordinates": null
  },
  "quality_score": 0.95,
  "processing_flags": ["official_source", "policy_announcement"]
}
```

### PropertyGuru Data Format

```json
{
  "id": "pg_review_789xyz",
  "source": "propertyguru",
  "scraped_at": "2024-01-15T10:30:00Z",
  "url": "https://www.propertyguru.com.sg/property-reviews/the-pinnacle-duxton-123456",
  "title": "Review: The Pinnacle @ Duxton",
  "content": "Lived here for 2 years. Great location but maintenance could be better...",
  "metadata": {
    "property_name": "The Pinnacle @ Duxton",
    "property_id": "123456",
    "review_type": "resident_review",
    "reviewer_profile": "verified_resident",
    "review_date": "2024-01-10T00:00:00Z",
    "helpful_votes": 15,
    "total_votes": 18,
    "property_type": "HDB",
    "district": "District 2",
    "tenure": "99-year leasehold",
    "completion_year": 2009,
    "facilities": ["gym", "swimming_pool", "playground"],
    "nearby_amenities": ["MRT", "shopping_mall", "schools"]
  },
  "sentiment": {
    "score": 0.1,
    "confidence": 0.80,
    "label": "neutral"
  },
  "rating": {
    "explicit": 3.5,
    "inferred": 3.2,
    "confidence": "high",
    "method": "explicit"
  },
  "location": {
    "mentioned_areas": ["Duxton", "Tanjong Pagar"],
    "property_types": ["HDB"],
    "coordinates": [1.2792, 103.8441]
  },
  "quality_score": 0.88,
  "processing_flags": ["verified_review", "detailed_content"]
}
```

### HardwareZone Data Format

```json
{
  "id": "hwz_edmw_post_456def",
  "source": "hardwarezone",
  "scraped_at": "2024-01-15T10:30:00Z",
  "url": "https://forums.hardwarezone.com.sg/eat-drink-man-woman-16/property-prices-going-crazy-6789012.html",
  "title": "Property prices going crazy or what?",
  "content": "Bro anyone notice property prices damn siao lately? My friend just bought resale HDB for 800k...",
  "metadata": {
    "forum_section": "Eat Drink Man Woman",
    "thread_id": "6789012",
    "post_number": 1,
    "author": "PropertyKaki88",
    "post_date": "2024-01-14T22:30:00Z",
    "thread_views": 2547,
    "thread_replies": 89,
    "author_join_date": "2019-03-15",
    "author_post_count": 1234,
    "thread_tags": ["property", "prices", "discussion"],
    "is_thread_starter": true,
    "quoted_posts": [],
    "attachments": []
  },
  "sentiment": {
    "score": -0.4,
    "confidence": 0.72,
    "label": "negative"
  },
  "rating": {
    "explicit": null,
    "inferred": 2.5,
    "confidence": "medium",
    "method": "sentiment"
  },
  "location": {
    "mentioned_areas": ["Singapore"],
    "property_types": ["HDB", "resale"],
    "coordinates": null
  },
  "quality_score": 0.65,
  "processing_flags": ["forum_discussion", "colloquial_language"]
}
```

## Data Quality Indicators

### Quality Score Calculation (0.0 - 1.0)

- **Content Length**: 0.2 weight
  - < 50 chars: 0.0
  - 50-200 chars: 0.5
  - 200+ chars: 1.0

- **Source Reliability**: 0.3 weight
  - Government: 1.0
  - Verified reviews: 0.9
  - Reddit posts: 0.7
  - Forum posts: 0.6

- **Sentiment Confidence**: 0.2 weight
  - High confidence (>0.8): 1.0
  - Medium confidence (0.5-0.8): 0.7
  - Low confidence (<0.5): 0.3

- **Location Specificity**: 0.15 weight
  - Specific area mentioned: 1.0
  - General Singapore: 0.5
  - No location: 0.0

- **Temporal Relevance**: 0.15 weight
  - Last 6 months: 1.0
  - 6-12 months: 0.8
  - 1-2 years: 0.6
  - >2 years: 0.3

### Processing Flags

Common flags across all sources:
- `duplicate_check_passed`: Content is unique
- `content_filtered`: Passed content quality filters
- `singapore_property_keywords`: Contains relevant property keywords
- `sufficient_length`: Meets minimum content length requirements
- `sentiment_analyzed`: Sentiment analysis completed
- `location_extracted`: Location information extracted
- `rating_inferred`: Rating successfully inferred

Source-specific flags:
- Reddit: `high_engagement`, `verified_user`, `detailed_discussion`
- Government: `official_source`, `policy_announcement`, `statistical_data`
- PropertyGuru: `verified_review`, `detailed_content`, `recent_review`
- HardwareZone: `forum_discussion`, `colloquial_language`, `thread_starter`

## File Naming Convention

```
{source}_{date}_{batch_number}.jsonl

Examples:
- reddit_20240115_001.jsonl
- government_20240115_001.jsonl
- propertyguru_20240115_001.jsonl
- hardwarezone_20240115_001.jsonl
```

## Data Validation Rules

1. **Required Fields**: All base schema fields must be present
2. **ID Uniqueness**: IDs must be unique within each source
3. **URL Validation**: URLs must be valid and accessible
4. **Date Format**: All dates in ISO 8601 format (UTC)
5. **Sentiment Score**: Must be between -1.0 and 1.0
6. **Rating Values**: Must be between 1.0 and 5.0 (if present)
7. **Quality Score**: Must be between 0.0 and 1.0
8. **Content Length**: Minimum 20 characters for meaningful analysis

## Processing Pipeline Integration

This format is designed to integrate seamlessly with the PropInsight processing pipeline:

1. **Raw Data Storage**: Direct output from scrapers
2. **Data Cleaning**: Standardization and deduplication
3. **Feature Engineering**: Extraction of additional features
4. **Model Training**: Input for sentiment and rating models
5. **Analysis**: Aggregation and trend analysis

## Storage Recommendations

- **Compression**: Use gzip compression for long-term storage
- **Partitioning**: Organize by source and date for efficient querying
- **Backup**: Regular backups with version control
- **Indexing**: Create indexes on frequently queried fields (source, scraped_at, location)