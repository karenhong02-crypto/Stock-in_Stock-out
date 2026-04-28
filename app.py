"""
app.py — AFA Stock Report Pipeline Web UI
Run locally:  streamlit run app.py
"""

import os, tempfile, zipfile
from io import BytesIO

import streamlit as st
from pipeline_core import run_pipeline

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="AFA Stock Pipeline",
    page_icon="📊",
    layout="wide",
)

# ── Session state init ────────────────────────────────────────────────────────
if "outputs" not in st.session_state:
    st.session_state.outputs = None    # dict: filename -> bytes
if "log_lines" not in st.session_state:
    st.session_state.log_lines = None
if "run_month" not in st.session_state:
    st.session_state.run_month = None

# ── Header ────────────────────────────────────────────────────────────────────
st.title("📊 AFA Monthly Stock Report Pipeline")
st.markdown(
    "Upload the required files, set the month, then click **Run Pipeline**. "
    "Processed reports will appear below for download."
)
st.divider()

# ── Month input ───────────────────────────────────────────────────────────────
month = st.text_input(
    "📅 Month Label",
    placeholder="e.g. April 2026",
    help="Used to label the output ZIP",
)
st.divider()

# ── File uploads ──────────────────────────────────────────────────────────────
left, right = st.columns(2)

with left:
    st.subheader("🗂️ Target Report Files")
    st.caption("Fresh monthly Excel files to be processed")
    pac_in   = st.file_uploader("AFA PAC Stock In.xlsx",   type=["xlsx"], key="pac_in")
    pac_out  = st.file_uploader("AFA PAC Stock Out.xlsx",  type=["xlsx"], key="pac_out")
    tech_in  = st.file_uploader("AFA Tech Stock In.xlsx",  type=["xlsx"], key="tech_in")
    tech_out = st.file_uploader("AFA Tech Stock Out.xlsx", type=["xlsx"], key="tech_out")
    master   = st.file_uploader("Master data (.csv or .csv.gz)", type=["csv", "gz"], key="master")

with right:
    st.subheader("📋 Reference Files  *(OneDrive exports — optional)*")
    st.caption("Used to fill in Supplier and Unit Price. Leave blank to fill from master data only.")
    ref_pac_in   = st.file_uploader("PAC Stock Movement In",   type=["xlsx"], key="ref_pac_in")
    ref_pac_out  = st.file_uploader("PAC Stock Movement Out",  type=["xlsx"], key="ref_pac_out")
    ref_tech_in  = st.file_uploader("Tech Stock Movement In",  type=["xlsx"], key="ref_tech_in")
    ref_tech_out = st.file_uploader("Tech Stock Movement Out", type=["xlsx"], key="ref_tech_out")

st.divider()

# ── Validation ────────────────────────────────────────────────────────────────
required = {
    "Month Label":        bool(month and month.strip()),
    "AFA PAC Stock In":   pac_in   is not None,
    "AFA PAC Stock Out":  pac_out  is not None,
    "AFA Tech Stock In":  tech_in  is not None,
    "AFA Tech Stock Out": tech_out is not None,
    "Master data":        master   is not None,
}
missing = [k for k, ok in required.items() if not ok]
ready   = len(missing) == 0
if missing:
    st.warning(f"⚠️  Still needed: **{', '.join(missing)}**")

# ── Run button ────────────────────────────────────────────────────────────────
if st.button("🚀 Run Pipeline", disabled=not ready, type="primary", use_container_width=True):
    with tempfile.TemporaryDirectory() as tmp:
        progress = st.progress(0, text="Saving uploaded files …")

        def save_upload(f, name):
            if f is None: return None
            path = os.path.join(tmp, name)
            with open(path, "wb") as out:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk: break
                    out.write(chunk)
            f.seek(0)
            return path

        save_upload(pac_in,   "AFA PAC Stock In.xlsx")
        save_upload(pac_out,  "AFA PAC Stock Out.xlsx")
        save_upload(tech_in,  "AFA Tech Stock In.xlsx")
        save_upload(tech_out, "AFA Tech Stock Out.xlsx")
        master_name = "Master data.csv.gz" if master.name.lower().endswith(".gz") else "Master data.csv"
        save_upload(master, master_name)

        rpi  = save_upload(ref_pac_in,   "ref_pac_in.xlsx")
        rpo  = save_upload(ref_pac_out,  "ref_pac_out.xlsx")
        rti  = save_upload(ref_tech_in,  "ref_tech_in.xlsx")
        rto  = save_upload(ref_tech_out, "ref_tech_out.xlsx")

        progress.progress(20, text="Running pipeline …")

        try:
            log_lines = run_pipeline(tmp, month.strip(), rpi, rpo, rti, rto)
            progress.progress(85, text="Reading output files …")

            # ── Read output files into memory so session_state can hold them ──
            output_names = [
                "AFA PAC Stock In.xlsx",
                "AFA PAC Stock Out.xlsx",
                "AFA Tech Stock In.xlsx",
                "AFA Tech Stock Out.xlsx",
            ]
            outputs = {}
            for name in output_names:
                path = os.path.join(tmp, name)
                if os.path.exists(path):
                    with open(path, "rb") as f:
                        outputs[name] = f.read()

            # Store in session_state so downloads persist across reruns
            st.session_state.outputs   = outputs
            st.session_state.log_lines = log_lines
            st.session_state.run_month = month.strip()

            progress.progress(100, text="Done!")
            progress.empty()

        except Exception as e:
            progress.empty()
            st.error(f"❌ Pipeline failed: {e}")
            with st.expander("🔍 Error details"):
                import traceback
                st.code(traceback.format_exc(), language="python")

# ── Show download section if outputs exist (persists across reruns) ───────────
if st.session_state.outputs:
    st.divider()
    st.success(f"✅ Pipeline complete for **{st.session_state.run_month}** — download below")

    # Build ZIP from in-memory bytes
    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in st.session_state.outputs.items():
            zf.writestr(name, data)
    zip_buf.seek(0)

    # ── Download all as ZIP ──
    st.download_button(
        label=f"⬇️  Download All as ZIP — {st.session_state.run_month}.zip",
        data=zip_buf,
        file_name=f"Stock Reports - {st.session_state.run_month}.zip",
        mime="application/zip",
        type="primary",
        use_container_width=True,
        key="zip_dl",
    )

    # ── Individual file downloads ──
    st.markdown("**Or download individually:**")
    cols = st.columns(len(st.session_state.outputs))
    for col, (name, data) in zip(cols, st.session_state.outputs.items()):
        with col:
            st.download_button(
                label=f"⬇️  {name.replace('.xlsx','')}",
                data=data,
                file_name=name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key=f"dl_{name}",
            )

    # ── Log ──
    with st.expander("📋 Pipeline Log", expanded=False):
        st.code("\n".join(st.session_state.log_lines or []), language="")

    # ── Reset button ──
    if st.button("🔄 Clear & start over"):
        st.session_state.outputs   = None
        st.session_state.log_lines = None
        st.session_state.run_month = None
        st.rerun()
