"""
app.py — AFA Stock Report Pipeline Web UI (memory-safe batch upload)
Each file is saved to disk immediately and removed from RAM before the next.
"""

import os, tempfile, zipfile, shutil
from io import BytesIO

import streamlit as st
from pipeline_core import run_pipeline

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="AFA Stock Pipeline", page_icon="📊", layout="wide")

# ── Per-session work directory ────────────────────────────────────────────────
if "work_dir" not in st.session_state:
    st.session_state.work_dir = tempfile.mkdtemp(prefix="afa_pipeline_")
if "uploader_nonce" not in st.session_state:
    st.session_state.uploader_nonce = {}     # bumped after save → resets widget
if "outputs" not in st.session_state:
    st.session_state.outputs = None
if "log_lines" not in st.session_state:
    st.session_state.log_lines = None
if "run_month" not in st.session_state:
    st.session_state.run_month = None

WORK = st.session_state.work_dir

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

def bump(slot): st.session_state.uploader_nonce[slot] = st.session_state.uploader_nonce.get(slot, 0) + 1

def stream_save(uploaded, target_name):
    """Write uploaded file to disk in 1 MB chunks → minimal RAM."""
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
    "**Memory-safe mode**: upload files **one at a time** and click 💾 Save after each. "
    "The file is moved to disk and your browser memory is freed before the next upload."
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

for slot, save_name, label, types, required in SLOTS:
    # The master slot may have been saved as .csv or .csv.gz
    saved_as = None
    if slot == "master":
        for n in ("Master data.csv.gz", "Master data.csv"):
            if on_disk(n): saved_as = n; break
    else:
        if on_disk(save_name): saved_as = save_name

    star = " *" if required else "  *(optional)*"

    if saved_as:
        col1, col2 = st.columns([5, 1])
        with col1:
            st.success(f"✅ **{label}**{star}  —  `{saved_as}`  ({file_size(saved_as)} saved)")
        with col2:
            if st.button("🗑️ Remove", key=f"rm_{slot}", use_container_width=True):
                delete_file(save_name, "Master data.csv.gz" if slot == "master" else save_name)
                st.rerun()
    else:
        col1, col2 = st.columns([5, 1])
        with col1:
            up = st.file_uploader(f"{label}{star}", type=types, key=widget_key(slot),
                                  label_visibility="visible")
        with col2:
            st.markdown("")  # spacer
            st.markdown("")
            disabled = up is None
            if st.button("💾 Save", key=f"save_{slot}", disabled=disabled, use_container_width=True):
                # Special-case master: keep .gz suffix if present
                target = save_name
                if slot == "master" and up.name.lower().endswith(".gz"):
                    target = "Master data.csv.gz"
                stream_save(up, target)
                bump(slot)              # reset uploader on next render
                st.rerun()

st.divider()

# ── Validation ────────────────────────────────────────────────────────────────
required_targets = ["AFA PAC Stock In.xlsx", "AFA PAC Stock Out.xlsx",
                    "AFA Tech Stock In.xlsx", "AFA Tech Stock Out.xlsx"]
master_ok = on_disk("Master data.csv") or on_disk("Master data.csv.gz")

missing = []
if not (month and month.strip()): missing.append("Month Label")
for n in required_targets:
    if not on_disk(n): missing.append(n.replace(".xlsx", ""))
if not master_ok: missing.append("Master data")

if missing:
    st.warning(f"⚠️  Still needed: **{', '.join(missing)}**")

# ── Run pipeline ──────────────────────────────────────────────────────────────
if st.button("🚀 Run Pipeline", disabled=bool(missing), type="primary", use_container_width=True):
    rpi = disk_path("ref_pac_in.xlsx")   if on_disk("ref_pac_in.xlsx")   else None
    rpo = disk_path("ref_pac_out.xlsx")  if on_disk("ref_pac_out.xlsx")  else None
    rti = disk_path("ref_tech_in.xlsx")  if on_disk("ref_tech_in.xlsx")  else None
    rto = disk_path("ref_tech_out.xlsx") if on_disk("ref_tech_out.xlsx") else None

    progress = st.progress(0, text="Running pipeline …")
    try:
        log_lines = run_pipeline(WORK, month.strip(), rpi, rpo, rti, rto)
        progress.progress(85, text="Reading output files …")

        outputs = {}
        for name in required_targets:
            p = disk_path(name)
            if os.path.exists(p):
                with open(p, "rb") as f:
                    outputs[name] = f.read()

        st.session_state.outputs   = outputs
        st.session_state.log_lines = log_lines
        st.session_state.run_month = month.strip()
        progress.progress(100, text="Done!")
        progress.empty()
        st.rerun()
    except Exception as e:
        progress.empty()
        st.error(f"❌ Pipeline failed: {e}")
        with st.expander("🔍 Error details"):
            import traceback
            st.code(traceback.format_exc(), language="python")

# ── Download section (persists across reruns) ─────────────────────────────────
if st.session_state.outputs:
    st.divider()
    st.success(f"✅ Pipeline complete for **{st.session_state.run_month}** — download below")

    # ZIP
    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in st.session_state.outputs.items():
            zf.writestr(name, data)
    zip_buf.seek(0)

    st.download_button(
        f"⬇️  Download All as ZIP — {st.session_state.run_month}.zip",
        data=zip_buf,
        file_name=f"Stock Reports - {st.session_state.run_month}.zip",
        mime="application/zip",
        type="primary",
        use_container_width=True,
        key="zip_dl",
    )

    st.markdown("**Or download individually:**")
    cols = st.columns(len(st.session_state.outputs))
    for col, (name, data) in zip(cols, st.session_state.outputs.items()):
        with col:
            st.download_button(
                f"⬇️  {name.replace('.xlsx','')}",
                data=data,
                file_name=name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key=f"dl_{name}",
            )

    with st.expander("📋 Pipeline Log", expanded=False):
        st.code("\n".join(st.session_state.log_lines or []), language="")

    if st.button("🔄 Clear & start over", use_container_width=True):
        # Wipe disk + state
        try: shutil.rmtree(WORK)
        except: pass
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()
