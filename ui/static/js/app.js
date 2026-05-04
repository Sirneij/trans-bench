/* global helpers used by base.html */

function toggleSidebar() {
  document.getElementById('sidebar').classList.toggle('open');
}

function showToast(msg, type = 'info') {
  const c = document.getElementById('toast-container');
  const t = document.createElement('div');
  t.className = `toast ${type}`;
  t.textContent = msg;
  c.appendChild(t);
  setTimeout(() => t.remove(), 4000);
}

// Close sidebar on outside click (mobile)
document.addEventListener('click', (e) => {
  const sb = document.getElementById('sidebar');
  const btn = document.querySelector('.menu-btn');
  if (sb && sb.classList.contains('open') && !sb.contains(e.target) && e.target !== btn) {
    sb.classList.remove('open');
  }
});
