"""
app.py — AFA Stock Report Pipeline Web UI (memory-safe batch upload)
Each file is saved to disk immediately and removed from RAM before the next.
"""

import os, tempfile, zipfile, shutil
from io import BytesIO

# ── Force-disable XSRF/CORS BEFORE Streamlit boots ────────────────────────────
# Must come before `import streamlit` — config is read at import time.
os.environ["STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION"] = "false"
os.environ["STREAMLIT_SERVER_ENABLE_CORS"] = "false"
os.environ["STREAMLIT_SERVER_MAX_UPLOAD_SIZE"] = "500"
os.environ["STREAMLIT_SERVER_MAX_MESSAGE_SIZE"] = "500"

import streamlit as st
from pipeline_core import run_pipeline

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="AFA Stock Pipeline", page_icon="📊", layout="wide")

# ── Persistent work directory (survives reruns and page refreshes) ────────────
# Cached at module level so it runs ONCE per container, not every rerun.
@st.cache_resource
def _resolve_work_dir():
    for candidate in ("/tmp/afa_pipeline_work",
                      os.path.join(tempfile.gettempdir(), "afa_pipeline_work")):
        try:
            os.makedirs(candidate, exist_ok=True)
            return candidate
        except Exception:
            continue
    return tempfile.mkdtemp(prefix="afa_pipeline_")

WORK = _resolve_work_dir()

# One-time startup log to verify config is loaded properly
@st.cache_resource
def _log_config_once():
    try:
        from streamlit import config as _cfg
        print(f"[config] maxUploadSize={_cfg.get_option('server.maxUploadSize')}", flush=True)
        print(f"[config] enableXsrfProtection={_cfg.get_option('server.enableXsrfProtection')}", flush=True)
        print(f"[config] enableCORS={_cfg.get_option('server.enableCORS')}", flush=True)
    except Exception as e:
        print(f"[config] failed: {e}", flush=True)
    return True
_log_config_once()
if "uploader_nonce" not in st.session_state:
    st.session_state.uploader_nonce = {}     # bumped after save → resets widget

# ── Helpers ───────────────────────────────────────────────────────────────────
def disk_path(name):  return os.path.join(WORK, name)
def on_disk(name):    return os.path.exists(disk_path(name))

def fmt_size(n):
    if n < 1024:           return f"{n} B"
    if n < 1024*1024:      return f"{n/1024:.1f} KB"
    return f"{n/1024/1024:.1f} MB"

def file_size(name):
    return fmt_size(os.path.getsize(disk_path(name))) if on_disk(name) else ""

def widget_key(slot): return f"up_{slot}_{st.session_state.uploader_nonce.get(slot, 0)}"

def bump(slot):
    """Increment the widget key so the next render gets a fresh, empty uploader."""
    st.session_state.uploader_nonce[slot] = st.session_state.uploader_nonce.get(slot, 0) + 1

def stream_save(uploaded, target_name):
    """Write uploaded file to disk in 1 MB chunks → minimal RAM."""
    os.makedirs(WORK, exist_ok=True)         # ensure work dir exists every save
    path = disk_path(target_name)
    with open(path, "wb") as out:
        while True:
            chunk = uploaded.read(1024 * 1024)
            if not chunk: break
            out.write(chunk)

def delete_file(*names):
    for n in names:
        p = disk_path(n)
        if os.path.exists(p): os.remove(p)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("📊 AFA Monthly Stock Report Pipeline")
st.caption(
    "Pick all your files in the slots below, then click **Save All Files** to upload them in one batch."
)
st.divider()

# ── Month input ───────────────────────────────────────────────────────────────
month = st.text_input("📅 Month Label", placeholder="e.g. April 2026")
st.divider()

# ── File slot definitions ─────────────────────────────────────────────────────
# (slot_id, save_name, ui_label, accepted_types, required)
SLOTS = [
    ("pac_in",   "AFA PAC Stock In.xlsx",   "AFA PAC Stock In",   ["xlsx"], True),
    ("pac_out",  "AFA PAC Stock Out.xlsx",  "AFA PAC Stock Out",  ["xlsx"], True),
    ("tech_in",  "AFA Tech Stock In.xlsx",  "AFA Tech Stock In",  ["xlsx"], True),
    ("tech_out", "AFA Tech Stock Out.xlsx", "AFA Tech Stock Out", ["xlsx"], True),
    ("master",   "Master data.csv",         "Master data (.csv or .csv.gz)", ["csv", "gz"], True),
    ("ref_pac_in",   "ref_pac_in.xlsx",   "PAC Stock Movement In  (reference)",   ["xlsx"], False),
    ("ref_pac_out",  "ref_pac_out.xlsx",  "PAC Stock Movement Out (reference)",   ["xlsx"], False),
    ("ref_tech_in",  "ref_tech_in.xlsx",  "Tech Stock Movement In  (reference)",  ["xlsx"], False),
    ("ref_tech_out", "ref_tech_out.xlsx", "Tech Stock Movement Out (reference)",  ["xlsx"], False),
]

st.subheader("📥 Upload files")
st.caption("Drop all the files you want to process into the slots below, then click **Save All Files**. They'll all be saved to disk in one batch.")

# ── Show files already on disk + Remove buttons ──
already_saved = []
for slot, save_name, label, types, required in SLOTS:
    saved_as = None
    if slot == "master":
        for n in ("Master data.csv.gz", "Master data.csv"):
            if on_disk(n): saved_as = n; break
    else:
        if on_disk(save_name): saved_as = save_name
    if saved_as:
        already_saved.append((slot, save_name, label, saved_as))

if already_saved:
    st.markdown("**Already saved:**")
    for slot, save_name, label, saved_as in already_saved:
        col1, col2 = st.columns([5, 1])
        with col1:
            st.success(f"✅ **{label}** — `{saved_as}` ({file_size(saved_as)})")
        with col2:
            if st.button("🗑️", key=f"rm_{slot}", use_container_width=True):
                delete_file(save_name, "Master data.csv.gz" if slot == "master" else save_name)
                st.rerun()

# ── Form-based batch upload (more reliable than auto-save on HF) ──
saved_slots = {s[0] for s in already_saved}
remaining = [s for s in SLOTS if s[0] not in saved_slots]

if remaining:
    st.markdown("**Upload more:**")
    with st.form(key=f"upload_form_{st.session_state.uploader_nonce.get('form', 0)}",
                 clear_on_submit=True):
        uploads = {}
        for slot, save_name, label, types, required in remaining:
            uploads[slot] = st.file_uploader(label, type=types, key=f"form_up_{slot}")
        submitted = st.form_submit_button("💾 Save All Files", type="primary",
                                          use_container_width=True)
        if submitted:
            n_saved = 0
            for slot, save_name, label, types, required in remaining:
                up = uploads.get(slot)
                if up is None: continue
                target = save_name
                if slot == "master" and up.name.lower().endswith(".gz"):
                    target = "Master data.csv.gz"
                stream_save(up, target)
                n_saved += 1
            st.session_state.uploader_nonce["form"] = st.session_state.uploader_nonce.get("form", 0) + 1
            if n_saved > 0:
                st.success(f"Saved {n_saved} file(s) to disk")
            st.rerun()

st.divider()

# ── Validation: only Month Label + at least one target file is needed ────────
required_targets = ["AFA PAC Stock In.xlsx", "AFA PAC Stock Out.xlsx",
                    "AFA Tech Stock In.xlsx", "AFA Tech Stock Out.xlsx"]

uploaded_targets = [n for n in required_targets if on_disk(n)]
month_ok         = bool(month and month.strip())
ready            = month_ok and len(uploaded_targets) > 0

if not month_ok:
    st.warning("⚠️  Please enter a **Month Label** above.")
elif not uploaded_targets:
    st.info("ℹ️  Upload at least **one target Excel file** to proceed.")
else:
    st.info(f"📦 Ready to process: **{', '.join(n.replace('.xlsx','') for n in uploaded_targets)}**")

# ── Run pipeline ──────────────────────────────────────────────────────────────
if st.button("🚀 Run Pipeline", disabled=not ready, type="primary", use_container_width=True):
    rpi = disk_path("ref_pac_in.xlsx")   if on_disk("ref_pac_in.xlsx")   else None
    rpo = disk_path("ref_pac_out.xlsx")  if on_disk("ref_pac_out.xlsx")  else None
    rti = disk_path("ref_tech_in.xlsx")  if on_disk("ref_tech_in.xlsx")  else None
    rto = disk_path("ref_tech_out.xlsx") if on_disk("ref_tech_out.xlsx") else None

    with st.spinner("Running pipeline … this may take 30–60 seconds"):
        try:
            log_lines = run_pipeline(WORK, month.strip(), rpi, rpo, rti, rto)

            # Build results.zip on disk — single source of truth
            with zipfile.ZipFile(disk_path("_results.zip"), "w", zipfile.ZIP_DEFLATED) as zf:
                for name in uploaded_targets:
                    src = disk_path(name)
                    if os.path.exists(src):
                        zf.write(src, name)

            # Persist month + log so the download section can read them after rerun
            with open(disk_path("_run_month.txt"), "w") as f:
                f.write(month.strip())
            with open(disk_path("_log.txt"), "w", encoding="utf-8") as f:
                f.write("\n".join(log_lines))
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            print(tb, flush=True)
            st.error(f"❌ Pipeline failed: {e}")
            with st.expander("🔍 Error details", expanded=True):
                st.code(tb, language="python")

# ── Download section — disk-backed, no session_state needed ───────────────────
if on_disk("_results.zip"):
    st.divider()
    run_month_path = disk_path("_run_month.txt")
    run_month = open(run_month_path).read().strip() if os.path.exists(run_month_path) else "Unknown"
    st.success(f"✅ Pipeline complete for **{run_month}** — download below")

    with open(disk_path("_results.zip"), "rb") as f:
        zip_bytes = f.read()

    st.download_button(
        f"⬇️  Download All as ZIP — {run_month}.zip",
        data=zip_bytes,
        file_name=f"Stock Reports - {run_month}.zip",
        mime="application/zip",
        type="primary",
        use_container_width=True,
        key="zip_dl",
    )

    # Individual downloads — read each file from disk
    individual = [n for n in ["AFA PAC Stock In.xlsx", "AFA PAC Stock Out.xlsx",
                              "AFA Tech Stock In.xlsx", "AFA Tech Stock Out.xlsx"]
                  if on_disk(n)]
    if individual:
        st.markdown("**Or download individually:**")
        cols = st.columns(len(individual))
        for col, name in zip(cols, individual):
            with col:
                with open(disk_path(name), "rb") as f:
                    st.download_button(
                        f"⬇️  {name.replace('.xlsx','')}",
                        data=f.read(),
                        file_name=name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        key=f"dl_{name}",
                    )

    log_path = disk_path("_log.txt")
    if os.path.exists(log_path):
        with st.expander("📋 Pipeline Log", expanded=False):
            with open(log_path, encoding="utf-8") as f:
                st.code(f.read(), language="")

    if st.button("🔄 Clear & start over", use_container_width=True):
        try: shutil.rmtree(WORK)
        except: pass
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()
