/* global helpers used by base.html */

function toggleSidebar() {
  document.getElementById('sidebar').classList.toggle('open');
}

function showToast(msg, type = 'info') {
  const c = document.getElementById('toast-container');
  const t = document.createElement('div');
  t.className = `toast ${type}`;
  const icons = { success: '✓', error: '✗', warn: '⚠', info: 'ℹ' };
  t.innerHTML = `<span class="toast-icon">${icons[type] || 'ℹ'}</span><span>${msg}</span>`;
  c.appendChild(t);
  setTimeout(() => { t.style.opacity = '0'; t.style.transform = 'translateY(10px)'; setTimeout(() => t.remove(), 300); }, 4000);
}

// Close sidebar on outside click (mobile)
document.addEventListener('click', (e) => {
  const sb = document.getElementById('sidebar');
  const btn = document.querySelector('.menu-btn');
  if (sb && sb.classList.contains('open') && !sb.contains(e.target) && e.target !== btn) {
    sb.classList.remove('open');
  }
});

// Initialise Lucide icons after DOM ready
document.addEventListener('DOMContentLoaded', () => {
  if (window.lucide) lucide.createIcons();
});
