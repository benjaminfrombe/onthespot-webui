#!/bin/bash
# Nuclear Session Reset Patch - Automated Installer
# For OnTheSpot session corruption fix

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Emoji support
CHECKMARK="✅"
CROSSMARK="❌"
ROCKET="🚀"
WRENCH="🔧"
INFO="ℹ️"

echo -e "${BLUE}=================================================${NC}"
echo -e "${BLUE}  Nuclear Session Reset Patch - Installer${NC}"
echo -e "${BLUE}=================================================${NC}"
echo ""

# Function to print colored messages
info() {
    echo -e "${BLUE}${INFO} $1${NC}"
}

success() {
    echo -e "${GREEN}${CHECKMARK} $1${NC}"
}

error() {
    echo -e "${RED}${CROSSMARK} $1${NC}"
}

warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

# Check if we're in the right directory
check_directory() {
    info "Checking OnTheSpot directory..."
    
    if [ -f "onthespot/api/spotify.py" ] && [ -f "onthespot/downloader.py" ]; then
        success "Found OnTheSpot files"
        return 0
    fi
    
    error "OnTheSpot files not found!"
    echo ""
    echo "Please run this script from the OnTheSpot root directory."
    echo "Expected files:"
    echo "  - onthespot/api/spotify.py"
    echo "  - onthespot/downloader.py"
    exit 1
}

# Check if patch file exists
check_patch_file() {
    info "Checking for patch file..."
    
    if [ ! -f "nuclear_session_reset.patch" ]; then
        error "Patch file 'nuclear_session_reset.patch' not found!"
        echo ""
        echo "Please place the patch file in the current directory."
        exit 1
    fi
    
    success "Patch file found"
}

# Backup original files
backup_files() {
    info "Creating backup of original files..."
    
    BACKUP_DIR="backup_$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$BACKUP_DIR"
    
    cp onthespot/api/spotify.py "$BACKUP_DIR/"
    cp onthespot/downloader.py "$BACKUP_DIR/"
    
    success "Backup created in $BACKUP_DIR"
    echo "  You can restore with: cp $BACKUP_DIR/* onthespot/"
}

# Apply patch
apply_patch() {
    info "Applying patch..."
    
    if command -v git &> /dev/null; then
        # Try git apply first
        if git apply nuclear_session_reset.patch 2>/dev/null; then
            success "Patch applied successfully with git"
            return 0
        else
            warning "git apply failed, trying patch command..."
        fi
    fi
    
    # Try patch command
    if command -v patch &> /dev/null; then
        if patch -p1 < nuclear_session_reset.patch; then
            success "Patch applied successfully with patch command"
            return 0
        fi
    fi
    
    error "Failed to apply patch automatically"
    echo ""
    echo "You can try applying manually:"
    echo "  1. Open the patch file"
    echo "  2. Follow the changes shown"
    echo "  3. Edit onthespot/api/spotify.py and onthespot/downloader.py"
    exit 1
}

# Verify installation
verify_installation() {
    info "Verifying installation..."
    
    # Check for key functions in spotify.py
    if grep -q "def _trigger_nuclear_reset" onthespot/downloader.py && \
       grep -q "def _cleanup_old_session" onthespot/api/spotify.py; then
        success "Verification passed - patch applied correctly"
        return 0
    else
        warning "Some functions may be missing"
        echo ""
        echo "Run verify_patch.py for detailed check:"
        echo "  python3 verify_patch.py"
    fi
}

# Run Python verification if available
run_python_verification() {
    if [ -f "verify_patch.py" ] && command -v python3 &> /dev/null; then
        info "Running detailed verification..."
        python3 verify_patch.py
    fi
}

# Print next steps
print_next_steps() {
    echo ""
    echo -e "${BLUE}=================================================${NC}"
    echo -e "${GREEN}${ROCKET} Installation Complete!${NC}"
    echo -e "${BLUE}=================================================${NC}"
    echo ""
    echo "Next steps:"
    echo ""
    echo "1. Restart OnTheSpot:"
    echo "   ${WRENCH} Stop the current process"
    echo "   ${WRENCH} Start OnTheSpot again"
    echo ""
    echo "2. Test the patch:"
    echo "   ${WRENCH} Queue multiple large albums"
    echo "   ${WRENCH} Watch logs for nuclear reset messages:"
    echo "       💥 TRIGGERING NUCLEAR RESET"
    echo "       ✓ NUCLEAR SESSION RESET SUCCESSFUL"
    echo ""
    echo "3. Monitor logs:"
    echo "   ${WRENCH} tail -f onthespot.log | grep -E '💥|🔥|✓|✗'"
    echo ""
    echo "If you see errors that previously required manual restart,"
    echo "they should now auto-recover with nuclear reset messages!"
    echo ""
    echo -e "${BLUE}=================================================${NC}"
}

# Main installation flow
main() {
    check_directory
    check_patch_file
    
    echo ""
    warning "This will modify OnTheSpot source files."
    read -p "Continue with installation? (y/n) " -n 1 -r
    echo ""
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Installation cancelled."
        exit 0
    fi
    
    echo ""
    backup_files
    echo ""
    apply_patch
    echo ""
    verify_installation
    echo ""
    run_python_verification
    echo ""
    print_next_steps
}

# Run main installation
main
