"""
app.py — AFA Stock Report Pipeline Web UI
Run locally:  streamlit run app.py
Deploy:       push to Render as a web service (start command: streamlit run app.py --server.port $PORT --server.address 0.0.0.0)
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

# ── Header ────────────────────────────────────────────────────────────────────
st.title("📊 AFA Monthly Stock Report Pipeline")
st.markdown(
    "Upload the required files, set the month, then click **Run Pipeline**. "
    "Processed reports will be available as a ZIP download."
)
st.divider()

# ── Month input ───────────────────────────────────────────────────────────────
month = st.text_input(
    "📅 Month Label",
    placeholder="e.g. April 2026",
    help="Must match the month shown on the OneDrive reference files.",
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
    master   = st.file_uploader("Master data.csv",         type=["csv"],  key="master")

with right:
    st.subheader("📋 Reference Files  *(OneDrive exports — optional)*")
    st.caption("Used to fill in Supplier and Unit Price. Leave blank to fill from master data only.")
    ref_pac_in   = st.file_uploader("PAC Stock Movement In",   type=["xlsx"], key="ref_pac_in")
    ref_pac_out  = st.file_uploader("PAC Stock Movement Out",  type=["xlsx"], key="ref_pac_out")
    ref_tech_in  = st.file_uploader("Tech Stock Movement In",  type=["xlsx"], key="ref_tech_in")
    ref_tech_out = st.file_uploader("Tech Stock Movement Out", type=["xlsx"], key="ref_tech_out")

st.divider()

# ── Validation ────────────────────────────────────────────────────────────────
required_fields = {
    "Month Label":          month,
    "AFA PAC Stock In":     pac_in,
    "AFA PAC Stock Out":    pac_out,
    "AFA Tech Stock In":    tech_in,
    "AFA Tech Stock Out":   tech_out,
    "Master data.csv":      master,
}
missing = [name for name, val in required_fields.items() if not val]
ready   = len(missing) == 0

if missing:
    st.warning(f"⚠️  Still needed: **{', '.join(missing)}**")

# ── Run button ────────────────────────────────────────────────────────────────
if st.button("🚀 Run Pipeline", disabled=not ready, type="primary", use_container_width=True):
    with tempfile.TemporaryDirectory() as tmp:
        progress = st.progress(0, text="Saving uploaded files …")

        # Save target files
        def save_upload(f, name):
            if f is None: return None
            path = os.path.join(tmp, name)
            with open(path, "wb") as out:
                out.write(f.getbuffer())
            return path

        save_upload(pac_in,   "AFA PAC Stock In.xlsx")
        save_upload(pac_out,  "AFA PAC Stock Out.xlsx")
        save_upload(tech_in,  "AFA Tech Stock In.xlsx")
        save_upload(tech_out, "AFA Tech Stock Out.xlsx")
        save_upload(master,   "Master data.csv")

        # Save optional reference files with fixed names
        rpi  = save_upload(ref_pac_in,   "ref_pac_in.xlsx")
        rpo  = save_upload(ref_pac_out,  "ref_pac_out.xlsx")
        rti  = save_upload(ref_tech_in,  "ref_tech_in.xlsx")
        rto  = save_upload(ref_tech_out, "ref_tech_out.xlsx")

        progress.progress(15, text="Running pipeline …")

        try:
            log_lines = run_pipeline(tmp, month, rpi, rpo, rti, rto)
            progress.progress(90, text="Packaging output files …")

            # Bundle outputs into a ZIP
            zip_buf = BytesIO()
            output_names = [
                "AFA PAC Stock In.xlsx",
                "AFA PAC Stock Out.xlsx",
                "AFA Tech Stock In.xlsx",
                "AFA Tech Stock Out.xlsx",
            ]
            with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for name in output_names:
                    path = os.path.join(tmp, name)
                    if os.path.exists(path):
                        zf.write(path, name)
            zip_buf.seek(0)

            progress.progress(100, text="Done!")
            st.success("✅ Pipeline completed successfully!")

            # Log expander
            with st.expander("📋 Pipeline Log", expanded=False):
                st.code("\n".join(log_lines), language="")

            # Download button
            st.download_button(
                label=f"⬇️  Download Processed Reports — {month}.zip",
                data=zip_buf,
                file_name=f"Stock Reports - {month}.zip",
                mime="application/zip",
                type="primary",
                use_container_width=True,
            )

        except Exception as e:
            progress.empty()
            st.error(f"❌ Pipeline failed: {e}")
            with st.expander("🔍 Error details"):
                import traceback
                st.code(traceback.format_exc(), language="python")
