#!/usr/bin/env python3
"""
Fix Tracker Database Tool

This script helps diagnose and fix issues with the tracker database.
It can:
1. Check for invalid file paths in the tracker database
2. Mark invalid entries as needing re-harvesting
3. Fix common path issues (like incorrect base paths)

Usage:
    python tools/fix_tracker.py [--verbose] [--auto-fix] [--dry-run]

Options:
    --verbose   Show detailed information about each issue
    --auto-fix  Automatically attempt to fix common issues
    --dry-run   Show what would be fixed without making changes
"""
import argparse
import logging
import sys
import os
import time
from pathlib import Path

# Add parent directory to path so we can import osf modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from osf import tracker, config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("fix_tracker")

def main():
    """Main function to fix tracker database issues."""
    parser = argparse.ArgumentParser(description="Fix issues with the tracker database")
    parser.add_argument('--verbose', action='store_true', help='Show detailed information')
    parser.add_argument('--auto-fix', action='store_true', help='Automatically attempt to fix common issues')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be fixed without making changes')
    args = parser.parse_args()
    
    # Initialize tracker database
    db = tracker.init_tracker_db()
    
    print(f"Tracker database: {config.TRACKER_DB_PATH}")
    
    # Validate file paths
    print("\nValidating file paths...")
    valid_count, invalid_count, total_count = tracker.validate_harvested_file_paths()
    
    print(f"Results:")
    print(f"- {total_count} total preprints in tracker")
    print(f"- {valid_count} preprints with valid file paths ({valid_count/total_count*100:.1f}%)")
    print(f"- {invalid_count} preprints with invalid file paths ({invalid_count/total_count*100:.1f}%)")
    
    if invalid_count == 0:
        print("\nNo issues found. All file paths are valid.")
        return
    
    # Get the invalid entries for detailed inspection
    if args.verbose or args.auto_fix or args.dry_run:
        print("\nAnalyzing invalid file paths...")
        results = db.query("SELECT id, file_path FROM harvested_preprints")
        invalid_entries = []
        
        for record in results:
            file_path = record.get("file_path")
            preprint_id = record.get("id")
            
            if not file_path or not Path(file_path).is_file():
                invalid_entries.append({
                    "id": preprint_id,
                    "file_path": file_path
                })
        
        # Analyze patterns in invalid paths
        path_patterns = {}
        for entry in invalid_entries:
            path = entry.get("file_path", "")
            base_dir = str(Path(path).parent) if path else "None"
            
            if base_dir in path_patterns:
                path_patterns[base_dir] += 1
            else:
                path_patterns[base_dir] = 1
        
        print("\nCommon path patterns in invalid entries:")
        for pattern, count in sorted(path_patterns.items(), key=lambda x: x[1], reverse=True):
            print(f"- {pattern}: {count} entries ({count/invalid_count*100:.1f}%)")
        
        if args.verbose:
            print("\nSample of invalid entries:")
            for i, entry in enumerate(invalid_entries[:10]):  # Show at most 10 examples
                print(f"  {i+1}. ID: {entry['id']}, Path: {entry['file_path']}")
            
            if len(invalid_entries) > 10:
                print(f"  ... and {len(invalid_entries) - 10} more")
        
        # Auto-fix logic
        if args.auto_fix or args.dry_run:
            print("\nChecking for common fixable issues...")
            
            # Find raw directory pattern
            raw_dir = config.RAW_DATA_DIR
            raw_dir_absolute = raw_dir.absolute()
            
            # Try to find valid base paths that might have been moved
            valid_base_path = None
            test_dirs = [
                raw_dir_absolute,
                Path("/opt/osfdata/data/raw"),
                Path("./data/raw"),
                Path("../data/raw")
            ]
            
            for test_dir in test_dirs:
                if test_dir.exists():
                    valid_base_path = test_dir
                    print(f"Found valid base directory: {valid_base_path}")
                    break
            
            if valid_base_path:
                # Count potential fixes
                fixable_count = 0
                for entry in invalid_entries:
                    old_path = Path(entry.get("file_path", ""))
                    if old_path.name.endswith(".json"):
                        # Try to construct a new path based on the original file name
                        relative_path = None
                        try:
                            # Extract the path components
                            components = old_path.parts
                            # Find where the data/raw part is
                            for i, part in enumerate(components):
                                if part == "raw" and i > 0 and components[i-1] == "data":
                                    # Get path relative to data/raw
                                    relative_path = Path(*components[i+1:])
                                    break
                        except Exception:
                            pass
                        
                        if relative_path:
                            new_path = valid_base_path / relative_path
                            if new_path.exists():
                                entry["new_path"] = str(new_path)
                                fixable_count += 1
                
                if fixable_count > 0:
                    print(f"\nFound {fixable_count} paths that can be fixed ({fixable_count/invalid_count*100:.1f}% of invalid)")
                    
                    if args.auto_fix and not args.dry_run:
                        print("\nApplying fixes...")
                        fixed_count = 0
                        
                        for entry in invalid_entries:
                            if "new_path" in entry:
                                try:
                                    db["harvested_preprints"].update(
                                        entry["id"], 
                                        {"file_path": entry["new_path"]}
                                    )
                                    fixed_count += 1
                                    
                                    # Show progress
                                    if fixed_count % 100 == 0:
                                        print(f"  Fixed {fixed_count}/{fixable_count} paths...")
                                except Exception as e:
                                    logger.error(f"Error fixing path for {entry['id']}: {e}")
                        
                        print(f"\nFixed {fixed_count} paths.")
                        
                        # Validate again
                        valid_count, invalid_count, total_count = tracker.validate_harvested_file_paths()
                        print(f"\nAfter fixing:")
                        print(f"- {valid_count} valid paths ({valid_count/total_count*100:.1f}%)")
                        print(f"- {invalid_count} invalid paths ({invalid_count/total_count*100:.1f}%)")
                    
                    elif args.dry_run:
                        print("\nDry run - showing sample of fixes that would be applied:")
                        for i, entry in enumerate(
                            [e for e in invalid_entries if "new_path" in e][:5]
                        ):
                            print(f"  {i+1}. ID: {entry['id']}")
                            print(f"     Old: {entry['file_path']}")
                            print(f"     New: {entry['new_path']}")
                        
                        remaining = fixable_count - min(5, fixable_count)
                        if remaining > 0:
                            print(f"  ... and {remaining} more")
                    
                    else:
                        print("\nUse --auto-fix to apply these fixes, or --dry-run to see more details.")
                else:
                    print("\nNo automatically fixable paths found.")
            else:
                print("\nCould not find a valid base directory for fixing paths.")
    
    # Final message
    print("\nTo fix remaining issues, you may need to:")
    print("1. Re-harvest the missing preprints")
    print("2. Manually fix the file paths in the tracker database")
    print("3. Reset the 'is_ingested' flag to force reprocessing")

if __name__ == "__main__":
    main() 