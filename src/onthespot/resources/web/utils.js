// utils.js

// Mobile Menu Toggle
function toggleMobileMenu(event) {
    event.preventDefault();
    const menu = document.getElementById('mobileMenu');
    menu.classList.toggle('show');
    
    // Close menu when clicking outside
    document.addEventListener('click', function closeMenu(e) {
        if (!e.target.closest('.mobile-menu-btn') && !e.target.closest('.mobile-menu')) {
            menu.classList.remove('show');
            document.removeEventListener('click', closeMenu);
        }
    });
}

function capitalizeFirstLetter(string) {
    if (!string) return 'N/A';
    return string.charAt(0).toUpperCase() + string.slice(1);
}

function copyToClipboard(text) {
    navigator.clipboard.writeText(text)
        .then(() => {
            console.log('Link copied to clipboard');
            showToast('Link copied to clipboard!', 'success');
        })
        .catch(err => {
            console.error('Failed to copy: ', err);
            showToast('Failed to copy link', 'error');
        });
}

function formatServiceName(serviceName) {
    const spacedServiceName = serviceName.replace(/_/g, ' ');

    const formattedServiceName = spacedServiceName.split(' ')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');

    return formattedServiceName;
}

function createButton(iconSrc, altText, onClickHandler, url = null) {
    if (url) {
        return `
            <button class="download-action-button" onclick="${onClickHandler}">
                <a href="${url}" onclick="event.preventDefault();">
                    <img src="${iconSrc}" loading="lazy" alt="${altText}">
                </a>
            </button>
        `;
    } else {
        return `
            <button class="download-action-button" onclick="${onClickHandler}">
                <img src="${iconSrc}" loading="lazy" alt="${altText}">
            </button>
        `;
    }
}

function updateSettings(data) {
    fetch('/api/update_settings', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(data),
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        return response.json();
    })
    .then(data => {
        console.log('Success:', data);
    })
    .catch((error) => {
        console.error('Error:', error);
    });
}

function toggleVisibility() {
    const div = document.getElementById('toggle_visibility');
    const img = document.getElementById('collapse_button_icon');
    // Check current display style and toggle
    if (div.style.display === 'none' || div.style.display === '') {
        div.style.display = 'block'; // Show the div
        img.src = '/icons/collapse_up.png'
    } else {
        div.style.display = 'none'; // Hide the div
        img.src = '/icons/collapse_down.png'
    }
}

// Global toast notification system
let _toastHideTimer = null;
function hideToast() {
    const toast = document.getElementById('toast');
    if (!toast) return;
    toast.classList.remove('visible');
    if (_toastHideTimer) {
        clearTimeout(_toastHideTimer);
        _toastHideTimer = null;
    }
}

function showToast(message, type = 'success', durationMs = 3000) {
    const toast = document.getElementById('toast');
    if (!toast) {
        console.warn('Toast element not found');
        return;
    }

    const icon = toast.querySelector('.toast-icon');
    const msg = toast.querySelector('.toast-message');

    if (icon && msg) {
        if (type === 'success') {
            icon.textContent = '✓';
        } else if (type === 'warning') {
            icon.textContent = '!';
        } else if (type === 'info') {
            icon.textContent = 'i';
        } else {
            icon.textContent = '✕';
        }
        msg.textContent = message;
    }

    toast.className = `toast ${type}`;
    toast.classList.add('visible');
    if (_toastHideTimer) {
        clearTimeout(_toastHideTimer);
        _toastHideTimer = null;
    }
    if (durationMs > 0) {
        _toastHideTimer = setTimeout(() => {
            toast.classList.remove('visible');
            _toastHideTimer = null;
        }, durationMs);
    }
}

// Add visual feedback to button clicks
function addButtonFeedback(button, originalText, loadingText = 'Processing...') {
    button.disabled = true;
    button.classList.add('button-loading');
    const originalBg = button.style.background;
    button.textContent = loadingText;

    return () => {
        button.disabled = false;
        button.classList.remove('button-loading');
        button.textContent = originalText;
        if (originalBg) button.style.background = originalBg;
    };
}

// Backend connection monitor (health poll + toast/banner)
let _connectionMonitorInitialized = false;
function initConnectionMonitor() {
    if (_connectionMonitorInitialized) return;
    _connectionMonitorInitialized = true;

    const banner = document.getElementById('connection-banner');
    if (!banner) return;

    const state = {
        down: false,
        lastToast: 0,
        currentMessage: '',
    };

    function setBanner(isDown) {
        if (isDown) {
            banner.classList.remove('hidden');
        } else {
            banner.classList.add('hidden');
        }
    }

    function maybeToast(message, type) {
        const now = Date.now();
        if (now - state.lastToast < 4000) return;
        state.lastToast = now;
        showToast(message, type);
    }

    function setBackendDown(reason = 'Reconnecting…') {
        const message = reason || 'Reconnecting…';
        if (!state.down) {
            state.down = true;
            setBanner(true);
        }
        if (state.currentMessage !== message) {
            state.currentMessage = message;
            showToast(message, 'warning', 0);
        }
    }

    function setBackendUp() {
        if (state.down) {
            state.down = false;
            setBanner(false);
            state.currentMessage = '';
            hideToast();
            showToast('Reconnected', 'success', 2500);
        }
    }

    window.otsConnectionMonitor = {
        setBackendDown,
        setBackendUp,
    };

    function pollHealth() {
        fetch('/__health', { cache: 'no-store' })
            .then((resp) => {
                if (resp.ok) {
                    setBackendUp();
                } else {
                    setBackendDown('Backend restarting…');
                }
            })
            .catch(() => {
                setBackendDown('Backend restarting…');
            })
            .finally(() => {
                setTimeout(pollHealth, 5000);
            });
    }

    window.addEventListener('offline', () => setBackendDown('Browser offline'));
    window.addEventListener('online', () => setBackendUp());

    pollHealth();
}

if (document.readyState === 'loading') {
    window.addEventListener('DOMContentLoaded', initConnectionMonitor);
} else {
    initConnectionMonitor();
}

// Global system notification poller (e.g. Plex scan completion)
let _notificationPollerInitialized = false;
function initNotificationPoller() {
    if (_notificationPollerInitialized) return;
    _notificationPollerInitialized = true;
    window.otsNotificationPollerActive = true;

    // Skip polling on login page to avoid unnecessary unauthorized requests.
    if (window.location.pathname === '/login') return;

    function fetchNotifications() {
        fetch('/api/notifications')
            .then((response) => {
                if (!response.ok) return null;
                return response.json();
            })
            .then((data) => {
                if (!data || !Array.isArray(data.notifications)) return;
                data.notifications.forEach((notif) => {
                    if (!notif || !notif.message) return;
                    showToast(notif.message, notif.type || 'info');
                });
            })
            .catch(() => {});
    }

    fetchNotifications();
    setInterval(fetchNotifications, 2000);
}

if (document.readyState === 'loading') {
    window.addEventListener('DOMContentLoaded', initNotificationPoller);
} else {
    initNotificationPoller();
}
