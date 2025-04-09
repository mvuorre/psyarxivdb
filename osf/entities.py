"""
OSF Preprints Entity Processing

Functions for processing different entities in preprint data and storing them in the SQLite database.
"""
import json
import logging

logger = logging.getLogger("osf.entities")

def process_provider(db, relationships):
    """Process and store provider information for a preprint.
    
    Args:
        db: sqlite-utils database instance
        relationships: Dictionary containing relationship data from API
    
    Returns:
        str: Provider ID if successful, None otherwise
    """
    provider_data = relationships.get('provider', {}).get('data', {})
    provider_id = provider_data.get('id')
    
    if provider_id:
        # Insert provider with just the ID
        db["providers"].upsert({
            "id": provider_id
        }, pk="id")
        return provider_id
    
    return None

def process_contributors(db, preprint_id, preprint_data):
    """Process and store contributors for a preprint.
    
    Args:
        db: sqlite-utils database instance
        preprint_id: The OSF ID of the preprint
        preprint_data: Dictionary containing preprint data from API
    """
    contributors = preprint_data.get('embeds', {}).get('contributors', {}).get('data', [])
    
    for idx, contributor in enumerate(contributors):
        try:
            # Extract the actual user ID from the relationships data
            user_data = contributor.get('relationships', {}).get('users', {}).get('data', {})
            contributor_id = user_data.get('id')
            
            # If we can't find the user ID in relationships, fall back to using the id from the user embed
            if not contributor_id:
                user_data = contributor.get('embeds', {}).get('users', {}).get('data', {})
                contributor_id = user_data.get('id')
                
            # If still no contributor_id, skip this contributor
            if not contributor_id:
                logger.warning(f"Could not find user ID for contributor in preprint {preprint_id}")
                continue
            
            # Extract attributes - either from the user embed or the contributor attributes
            user_attributes = contributor.get('embeds', {}).get('users', {}).get('data', {}).get('attributes', {})
            contributor_attributes = contributor.get('attributes', {})
            
            # Get links and social data
            user_links = contributor.get('embeds', {}).get('users', {}).get('data', {}).get('links', {})
            social_data = user_attributes.get('social', {})
            
            # Build the contributor record
            contributor_record = {
                "id": contributor_id,
                "full_name": user_attributes.get('full_name'),
                "given_name": user_attributes.get('given_name'),
                "middle_names": user_attributes.get('middle_names'),
                "family_name": user_attributes.get('family_name'),
                "suffix": user_attributes.get('suffix'),
                "date_registered": user_attributes.get('date_registered'),
                "active": 1 if user_attributes.get('active') else 0,
                "timezone": user_attributes.get('timezone'),
                "locale": user_attributes.get('locale'),
                "orcid": social_data.get('orcid'),
                "github": social_data.get('github'),
                "scholar": social_data.get('scholar'),
                "profile_websites": json.dumps(social_data.get('profileWebsites', [])),
                "employment": json.dumps(user_attributes.get('employment', [])),
                "education": json.dumps(user_attributes.get('education', [])),
                "profile_url": user_links.get('html')
            }
            
            # Insert into contributors table
            db["contributors"].upsert(contributor_record, pk="id")
            
            # Create preprint-contributor relationship
            db["preprint_contributors"].upsert({
                "preprint_id": preprint_id,
                "contributor_id": contributor_id,
                "contributor_index": idx,
                "bibliographic": 1 if contributor_attributes.get('bibliographic', True) else 0
            }, pk=("preprint_id", "contributor_id"))
        
        except Exception as e:
            logger.warning(f"Error processing contributor for preprint {preprint_id}: {e}")

def process_subjects(db, preprint_id, preprint_data):
    """Process and store subjects for a preprint.
    
    Args:
        db: sqlite-utils database instance
        preprint_id: The OSF ID of the preprint
        preprint_data: Dictionary containing preprint data from API
    """
    # Get subjects from attributes (which is a nested array structure)
    subject_lists = preprint_data.get('attributes', {}).get('subjects', [])
    
    # Process each subject hierarchy (subject_lists is a list of lists)
    for subject_group in subject_lists:
        for subject in subject_group:
            try:
                subject_id = subject.get('id')
                subject_text = subject.get('text')
                
                if not subject_id:
                    continue
                
                # Insert subject if it doesn't exist
                db["subjects"].upsert({
                    "id": subject_id,
                    "text": subject_text
                }, pk="id")
                
                # Create preprint-subject relationship
                db["preprint_subjects"].upsert({
                    "preprint_id": preprint_id,
                    "subject_id": subject_id
                }, pk=("preprint_id", "subject_id"))
            
            except Exception as e:
                logger.warning(f"Error processing subject {subject.get('id')} for preprint {preprint_id}: {e}")

def process_tags(db, preprint_id, tags):
    """Process and store tags for a preprint.
    
    Args:
        db: sqlite-utils database instance
        preprint_id: The OSF ID of the preprint
        tags: List of tag strings
    """
    try:
        # Process all tags, splitting any that contain commas or semicolons
        all_tags = []
        for tag in tags:
            if not tag or not isinstance(tag, str):
                continue
            
            tag = tag.strip()
            
            # Check if the tag contains separators (commas or semicolons)
            if ',' in tag or ';' in tag:
                # Replace semicolons with commas for consistent splitting
                tag = tag.replace(';', ',')
                # Split by comma and clean each tag
                split_tags = [t.strip() for t in tag.split(',') if t.strip()]
                all_tags.extend(split_tags)
            else:
                all_tags.append(tag)
        
        # Remove duplicates and sort
        unique_tags = sorted(set(all_tags))
        
        # Store tags as JSON array in the preprints table
        db["preprints"].update(
            preprint_id,
            {"tags": json.dumps(unique_tags)}
        )
    
    except Exception as e:
        logger.warning(f"Error processing tags for preprint {preprint_id}: {e}")

def extract_preprint_data(preprint_id, preprint_data):
    """Extract core preprint data from the API response.
    
    Args:
        preprint_id: The OSF ID of the preprint
        preprint_data: Dictionary containing preprint data from API
        
    Returns:
        dict: Dictionary containing normalized preprint data
    """
    attributes = preprint_data.get('attributes', {})
    relationships = preprint_data.get('relationships', {})
    links = preprint_data.get('links', {})
    
    return {
        "id": preprint_id,
        "title": attributes.get('title'),
        "description": attributes.get('description'),
        "date_created": attributes.get('date_created'),
        "date_modified": attributes.get('date_modified'),
        "date_published": attributes.get('date_published'),
        "original_publication_date": attributes.get('original_publication_date'),
        "publication_doi": f"https://doi.org/{attributes.get('doi')}" if attributes.get('doi') else None,
        "provider": relationships.get('provider', {}).get('data', {}).get('id'),
        "is_published": 1 if attributes.get('is_published') else 0,
        "reviews_state": attributes.get('reviews_state'),
        "version": attributes.get('version'),
        "is_latest_version": 1 if attributes.get('is_latest_version', True) else 0,
        "download_url": f"https://osf.io/download/{relationships.get('primary_file', {}).get('data', {}).get('id')}" if relationships.get('primary_file', {}).get('data') else None,
        "preprint_doi": links.get('preprint_doi'),
        "license": preprint_data.get('embeds', {}).get('license', {}).get('data', {}).get('attributes', {}).get('name'),
        "tags": json.dumps([]),  # Default empty tags array
        "has_coi": 1 if attributes.get('has_coi') else 0,
        "conflict_of_interest_statement": attributes.get('conflict_of_interest_statement'),
        "has_data_links": attributes.get('has_data_links'),
        "why_no_data": attributes.get('why_no_data'),
        "data_links": json.dumps(attributes.get('data_links', [])),
        "has_prereg_links": attributes.get('has_prereg_links'),
        "why_no_prereg": attributes.get('why_no_prereg'),
        "prereg_links": json.dumps(attributes.get('prereg_links', [])),
        "prereg_link_info": attributes.get('prereg_link_info')
    } 