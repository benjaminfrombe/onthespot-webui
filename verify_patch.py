#!/usr/bin/env python3
"""
Nuclear Reset Patch Verification Script
Tests if the patch has been properly applied to OnTheSpot
"""

import sys
import os
from pathlib import Path

def check_file_exists(filepath):
    """Check if file exists"""
    if not Path(filepath).exists():
        print(f"❌ File not found: {filepath}")
        return False
    print(f"✅ File found: {filepath}")
    return True

def check_function_exists(filepath, function_name):
    """Check if function exists in file"""
    try:
        with open(filepath, 'r') as f:
            content = f.read()
            if f"def {function_name}" in content:
                print(f"✅ Function '{function_name}' found in {filepath}")
                return True
            else:
                print(f"❌ Function '{function_name}' NOT found in {filepath}")
                return False
    except Exception as e:
        print(f"❌ Error reading {filepath}: {e}")
        return False

def check_import_exists(filepath, import_statement):
    """Check if import exists in file"""
    try:
        with open(filepath, 'r') as f:
            content = f.read()
            if import_statement in content:
                print(f"✅ Import '{import_statement}' found in {filepath}")
                return True
            else:
                print(f"❌ Import '{import_statement}' NOT found in {filepath}")
                return False
    except Exception as e:
        print(f"❌ Error reading {filepath}: {e}")
        return False

def main():
    print("=" * 60)
    print("Nuclear Reset Patch Verification")
    print("=" * 60)
    print()
    
    # Find OnTheSpot directory
    onthespot_dirs = [
        "onthespot",
        "/app/onthespot",
        "/app/venv/lib/python3.14/site-packages/onthespot",
        "../onthespot",
        "../../onthespot",
    ]
    
    base_dir = None
    for dir_path in onthespot_dirs:
        if Path(dir_path).exists() and Path(f"{dir_path}/api/spotify.py").exists():
            base_dir = dir_path
            break
    
    if not base_dir:
        print("❌ Could not find OnTheSpot directory!")
        print("   Please run this script from OnTheSpot root directory")
        return False
    
    print(f"📁 OnTheSpot directory: {base_dir}")
    print()
    
    # Files to check
    spotify_api_file = f"{base_dir}/api/spotify.py"
    downloader_file = f"{base_dir}/downloader.py"
    
    # Check files exist
    print("Checking files...")
    checks_passed = 0
    checks_total = 0
    
    checks_total += 1
    if check_file_exists(spotify_api_file):
        checks_passed += 1
    
    checks_total += 1
    if check_file_exists(downloader_file):
        checks_passed += 1
    
    print()
    
    # Check new functions in spotify.py
    print("Checking new functions in spotify.py...")
    new_functions_spotify = [
        "_halt_downloads_for_account",
        "_resume_downloads_for_account",
        "is_account_halted",
        "_cleanup_old_session",
        "_validate_session",
    ]
    
    for func in new_functions_spotify:
        checks_total += 1
        if check_function_exists(spotify_api_file, func):
            checks_passed += 1
    
    print()
    
    # Check new functions in downloader.py
    print("Checking new functions in downloader.py...")
    new_functions_downloader = [
        "_trigger_nuclear_reset",
        "_should_trigger_nuclear_reset",
    ]
    
    for func in new_functions_downloader:
        checks_total += 1
        if check_function_exists(downloader_file, func):
            checks_passed += 1
    
    print()
    
    # Check imports
    print("Checking new imports...")
    checks_total += 1
    if check_import_exists(spotify_api_file, "import gc"):
        checks_passed += 1
    
    print()
    
    # Summary
    print("=" * 60)
    print(f"Results: {checks_passed}/{checks_total} checks passed")
    print("=" * 60)
    
    if checks_passed == checks_total:
        print("✅ SUCCESS! Nuclear Reset Patch is properly installed!")
        print()
        print("Next steps:")
        print("1. Restart OnTheSpot")
        print("2. Queue some downloads")
        print("3. Watch logs for nuclear reset messages (💥 🔥 ✓)")
        return True
    else:
        print("❌ INCOMPLETE! Some patch components are missing.")
        print()
        print("Troubleshooting:")
        print("1. Check if patch was applied correctly")
        print("2. Try: git apply nuclear_session_reset.patch")
        print("3. Or apply changes manually using the patch as reference")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
